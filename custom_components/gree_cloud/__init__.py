"""The Gree Climate Cloud integration."""

from __future__ import annotations

import asyncio
import logging
import ssl

import greeclimate.mqtt_client as _gc_mqtt_client
from greeclimate.cloud_api import GreeCloudApi
from greeclimate.mqtt_client import GreeMqttClient

from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant

from .const import CONF_SERVER, DOMAIN, GREE_MQTT_SERVERS
from .coordinator import (
    CloudDiscoveryService,
    GreeCloudConfigEntry,
    GreeCloudRuntimeData,
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.CLIMATE, Platform.SWITCH, Platform.WATER_HEATER]

# Substrings that identify an MQTT "not connected" error from paho / aiomqtt.
_MQTT_DISCONNECTED_MARKERS = ("code:4", "not currently connected", "not connected")

# ---------------------------------------------------------------------------
# Blocking-call fix
#
# greeclimate's GreeMqttClient.connect() calls ssl.create_default_context()
# synchronously *inside* the event loop every time it (re)connects. That
# performs blocking disk I/O (loading the system cert store) and trips
# Home Assistant's "Detected blocking call" guard - and, worse, adds real
# latency to every connect/reconnect, which is one of the things that can
# push a slow device's bind() past its timeout.
#
# We build the TLS context once, off the event loop via the executor, cache
# it, and monkeypatch ssl.create_default_context inside greeclimate's module
# so every future call (first connect and every reconnect) gets the cached
# context instantly instead of touching disk again.
# ---------------------------------------------------------------------------
_cached_tls_context: ssl.SSLContext | None = None
_tls_patch_applied = False


def _build_tls_context() -> ssl.SSLContext:
    """Build the TLS context (runs in executor, off the event loop)."""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


async def _ensure_tls_context_cached(hass: HomeAssistant) -> None:
    """Pre-warm and cache the TLS context, and patch greeclimate to reuse it."""
    global _cached_tls_context, _tls_patch_applied

    if _cached_tls_context is None:
        _cached_tls_context = await hass.async_add_executor_job(_build_tls_context)
        _LOGGER.debug("Pre-warmed and cached Gree Cloud TLS context")

    if not _tls_patch_applied:
        def _patched_create_default_context(*_args, **_kwargs) -> ssl.SSLContext:
            return _cached_tls_context

        _gc_mqtt_client.ssl.create_default_context = _patched_create_default_context
        _tls_patch_applied = True
        _LOGGER.debug("Patched greeclimate.mqtt_client to reuse cached TLS context")


async def async_reconnect_mqtt(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Re-establish the MQTT connection after a broker disconnect.

    Returns True if the reconnect succeeded, False otherwise.
    Acquires the per-entry lock so that concurrent poll cycles don't each
    try to reconnect simultaneously.
    """
    runtime = entry.runtime_data
    lock = runtime.mqtt_reconnect_lock

    if lock.locked():
        # Another coroutine is already reconnecting — wait for it to finish,
        # then return (the client will already be fresh).
        async with lock:
            pass
        return runtime.mqtt_client.is_connected

    async with lock:
        _LOGGER.warning("MQTT disconnected — attempting to reconnect")

        old_client = runtime.mqtt_client
        mqtt_server = GREE_MQTT_SERVERS.get(entry.data[CONF_SERVER], "mqtt-eu.gree.com")

        try:
            # Re-login to get a fresh token (tokens can expire).
            credentials = await runtime.cloud_api.login()

            await _ensure_tls_context_cached(hass)

            new_client = GreeMqttClient(
                credentials.user_id,
                credentials.token,
                server=mqtt_server,
            )
            await new_client.connect()
        except Exception as err:
            _LOGGER.error("MQTT reconnect failed during connect: %s", err)
            return False

        # Swap the client reference on every device and re-subscribe.
        for coordinator in runtime.coordinators:
            device = coordinator.device
            try:
                # Remove the old handler registered against the old client.
                old_client.remove_message_handler(device._handle_mqtt_message)
            except Exception:
                pass
            device._mqtt_client = new_client
            new_client.add_message_handler(device._handle_mqtt_message)
            try:
                # bind() re-subscribes to response/status/connect topics.
                await device.bind()
            except Exception as err:
                _LOGGER.warning(
                    "Failed to re-bind device %s after reconnect: %s",
                    device.device_info.name,
                    err,
                )

        runtime.mqtt_client = new_client

        # Best-effort cleanup of the old client.
        try:
            await old_client.disconnect()
        except Exception:
            pass

        _LOGGER.info("MQTT reconnect successful")
        return True


async def _run_discovery_background(
    hass: HomeAssistant,
    entry: GreeCloudConfigEntry,
    discovery: CloudDiscoveryService,
    mqtt_client: GreeMqttClient,
) -> None:
    """Discover and bind all cloud devices in the background.

    Runs *after* async_setup_entry has already returned, so it can take as
    long as it needs (hundreds of devices, retries, paced batches) without
    ever risking a Home Assistant bootstrap timeout. Platforms are already
    listening on DISPATCH_DEVICE_DISCOVERED by the time this runs, so each
    device becomes a working entity as soon as it's bound — no need to wait
    for the whole fleet.
    """
    try:
        coordinators = await discovery.discover_devices(mqtt_client)
        entry.runtime_data.coordinators.extend(coordinators)
        _LOGGER.info(
            "Gree Cloud: background discovery finished, %d device(s) ready",
            len(coordinators),
        )
    except Exception:
        _LOGGER.exception("Gree Cloud: background discovery task failed unexpectedly")


async def _async_options_updated(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> None:
    """Reload the entry when its options (batch size, concurrency, ...) change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_setup_entry(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Set up Gree Climate Cloud from a config entry.

    This intentionally does the minimum needed to come up fast: login,
    connect MQTT, forward platforms. Device discovery/binding — which can
    take a long time on large fleets (100+ devices) — runs afterwards as a
    background task so it can never cause a "Bootstrap stage 2 timeout".
    """
    _LOGGER.info("Setting up Gree Climate Cloud integration")

    try:
        # Create Cloud API client
        api = GreeCloudApi.for_server(
            entry.data[CONF_SERVER],
            entry.data[CONF_USERNAME],
            entry.data[CONF_PASSWORD],
        )

        # Login to cloud
        _LOGGER.debug("Logging in to Gree Cloud")
        credentials = await api.login()

        # Create MQTT client
        _LOGGER.debug("Connecting to Gree MQTT broker")
        mqtt_server = GREE_MQTT_SERVERS.get(entry.data[CONF_SERVER], "mqtt-eu.gree.com")
        if entry.data[CONF_SERVER] not in GREE_MQTT_SERVERS:
            _LOGGER.warning(
                "Unknown server region '%s', falling back to Europe MQTT server",
                entry.data[CONF_SERVER],
            )

        await _ensure_tls_context_cached(hass)

        mqtt_client = GreeMqttClient(credentials.user_id, credentials.token, server=mqtt_server)
        await mqtt_client.connect()

        # Store runtime data. coordinators starts empty and is populated
        # incrementally by the background discovery task below. The poll
        # semaphore bounds how many *ongoing* status polls (once the
        # integration is loaded and devices are on their normal timers) can
        # be in flight to Gree's cloud at once.
        from .const import CONF_POLL_CONCURRENCY_LIMIT, DEFAULT_POLL_CONCURRENCY_LIMIT

        poll_concurrency_limit = entry.options.get(
            CONF_POLL_CONCURRENCY_LIMIT, DEFAULT_POLL_CONCURRENCY_LIMIT
        )
        entry.runtime_data = GreeCloudRuntimeData(
            cloud_api=api,
            mqtt_client=mqtt_client,
            coordinators=[],
            poll_semaphore=asyncio.Semaphore(max(1, poll_concurrency_limit)),
        )

        # Setup platforms *first* so their dispatcher listeners are already
        # registered before any device gets discovered/bound — otherwise an
        # early device could be dispatched before anyone is listening.
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        # Reload automatically if the user changes options (batch size etc.)
        entry.async_on_unload(entry.add_update_listener(_async_options_updated))

        # Kick off discovery/binding of every device in the background.
        # entry.async_create_background_task also ensures this task is
        # cancelled automatically if the entry is unloaded/reloaded.
        discovery = CloudDiscoveryService(hass, entry, api)
        entry.async_create_background_task(
            hass,
            _run_discovery_background(hass, entry, discovery, mqtt_client),
            name=f"gree_cloud_discovery_{entry.entry_id}",
        )

        _LOGGER.info(
            "Gree Climate Cloud set up; device discovery continues in the background"
        )

        return True

    except Exception as err:
        _LOGGER.exception("Failed to setup Gree Climate Cloud: %s", err)
        return False


async def async_unload_entry(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading Gree Climate Cloud integration")

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        # Close all devices
        for coordinator in entry.runtime_data.coordinators:
            try:
                await coordinator.device.close()
            except Exception as err:
                _LOGGER.warning("Error closing device: %s", err)

        # Disconnect MQTT client
        try:
            await entry.runtime_data.mqtt_client.disconnect()
        except Exception as err:
            _LOGGER.warning("Error disconnecting MQTT client: %s", err)

        # Close API session
        try:
            await entry.runtime_data.cloud_api.close()
        except Exception as err:
            _LOGGER.warning("Error closing API session: %s", err)

    return unload_ok
