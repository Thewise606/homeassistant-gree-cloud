"""Helper and wrapper classes for Gree Cloud module."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass
from datetime import timedelta
import logging
from typing import Any

from greeclimate.cloud_api import CloudDeviceInfo, GreeCloudApi
from greeclimate.cloud_device import CloudDevice
from greeclimate.device import Props
from greeclimate.deviceinfo import DeviceInfo
from greeclimate.mqtt_client import GreeMqttClient

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

# Imported lazily at call time to avoid circular import (coordinator <- __init__).
_RECONNECT_FUNC_NAME = "async_reconnect_mqtt"
_RECONNECT_MODULE = __name__.rsplit(".", 1)[0]  # custom_components.gree_cloud

from .const import (
    CONF_SERVER,
    DEFAULT_POLL_CONCURRENCY_LIMIT,
    DISPATCH_DEVICE_DISCOVERED,
    DOMAIN,
    HWHP_PROP_POW_CONSUMP,
    HWHP_PROP_SET_TEM_DEC,
    HWHP_PROP_SET_TEM_INT,
    HWHP_PROP_WATER_TEMP,
    HWHP_PROP_WSTATE,
    MAX_ERRORS,
    UPDATE_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

# Extra raw properties requested from the device in addition to the standard Props enum.
# These cover Hot Water Heat Pump (HWHP) devices that expose different sensor keys.
_STANDARD_PROPS: list[str] = [x.value for x in Props]
_HWHP_EXTRA_PROPS = [
    HWHP_PROP_WATER_TEMP,
    HWHP_PROP_SET_TEM_INT,
    HWHP_PROP_SET_TEM_DEC,
    HWHP_PROP_WSTATE,
    HWHP_PROP_POW_CONSUMP,
]


class HWHPAwareCloudDevice(CloudDevice):
    """CloudDevice subclass that also requests HWHP-specific properties.

    The Gree WHIO Hot Water Heat Pump reports current water temperature under
    the ``WatTem`` property key which is not part of the standard ``Props``
    enum.  This subclass overrides ``update_state`` to include that key in the
    status request so it is stored in ``raw_properties`` and can be read by the
    water_heater entity.
    """

    async def update_state(self) -> None:
        """Update device state, including HWHP-specific properties."""
        _LOGGER.debug(
            "Updating HWHP-aware cloud device state: %s", self.device_info.name
        )

        props: list[str] = _STANDARD_PROPS + _HWHP_EXTRA_PROPS
        if not self.hid:
            props.append("hid")

        self._response_event = asyncio.Event()
        self._response_data = None

        command = {"t": "status", "cols": props}

        await self._mqtt_client.publish_command(
            self._parent_mac,
            command,
            self.device_cipher,
            self._child_mac,
        )

        try:
            await asyncio.wait_for(
                self._response_event.wait(), timeout=self._command_timeout
            )
            if self._response_data:
                self.handle_state_update(**self._response_data)
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Timeout waiting for state update from %s", self.device_info.name
            )
        finally:
            self._response_event = None
            self._response_data = None


def is_hwhp_device(coordinator: "CloudDeviceDataUpdateCoordinator") -> bool:
    """Return True if the device appears to be a Hot Water Heat Pump.

    Detection requires a positive WatTmp raw value (actual = raw - 100).
    Standard AC units return 0 for unknown properties; a real HWHP reports
    actual water temperature (40–80 °C → raw 140–180), so raw > 0 is the
    discriminator.
    """
    raw = coordinator.device.raw_properties.get(HWHP_PROP_WATER_TEMP)
    return raw is not None and raw > 0


def _is_mqtt_disconnected(error: Exception) -> bool:
    """Return True if *error* indicates the MQTT client is not connected."""
    msg = str(error).lower()
    return any(m in msg for m in ("code:4", "not currently connected", "not connected"))


async def _try_reconnect(hass: HomeAssistant, entry: "GreeCloudConfigEntry") -> bool:
    """Lazy-import and call async_reconnect_mqtt to avoid circular imports."""
    import importlib
    mod = importlib.import_module(_RECONNECT_MODULE)
    return await mod.async_reconnect_mqtt(hass, entry)


type GreeCloudConfigEntry = ConfigEntry[GreeCloudRuntimeData]


@dataclass
class GreeCloudRuntimeData:
    """Runtime data for Gree Climate Cloud integration."""

    cloud_api: GreeCloudApi
    mqtt_client: GreeMqttClient
    coordinators: list[CloudDeviceDataUpdateCoordinator]
    mqtt_reconnect_lock: asyncio.Lock = None
    # Shared across every device's coordinator so that, once the integration
    # is fully loaded and each device is polling on its own independent
    # 60s timer, only a bounded number of status polls / commands are ever
    # in flight to Gree's cloud at once — even on a fleet of 100+ devices
    # whose timers happen to line up.
    poll_semaphore: asyncio.Semaphore = None

    def __post_init__(self) -> None:
        """Initialise fields that need a running event loop."""
        if self.mqtt_reconnect_lock is None:
            self.mqtt_reconnect_lock = asyncio.Lock()
        if self.poll_semaphore is None:
            self.poll_semaphore = asyncio.Semaphore(DEFAULT_POLL_CONCURRENCY_LIMIT)


class CloudDeviceDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Manages polling for state changes from cloud devices."""

    config_entry: GreeCloudConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: GreeCloudConfigEntry,
        device: CloudDevice,
    ) -> None:
        """Initialize the cloud data update coordinator."""
        # Deterministic per-device jitter (0-20s) added to the base update
        # interval. On a large fleet, many coordinators otherwise end up on
        # nearly the same 60s heartbeat (they were all created within a
        # short discovery window), causing periodic polling to burst
        # together the same way initial discovery would without batching.
        try:
            jitter = int(device.device_info.mac, 16) % 20 if device.device_info.mac else 0
        except ValueError:
            jitter = 0

        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN}-{device.device_info.name}",
            update_interval=timedelta(seconds=UPDATE_INTERVAL + jitter),
            always_update=False,
        )
        self.device = device
        self._error_count: int = 0
        self._poll_semaphore = config_entry.runtime_data.poll_semaphore

    async def _async_update_data(self) -> dict[str, Any]:
        """Update the state of the device from cloud."""
        _LOGGER.debug(
            "Updating cloud device state: %s, error count: %d",
            self.name,
            self._error_count,
        )
        try:
            # Bounded so a large fleet's independently-timed polls can't all
            # hit Gree's cloud/MQTT broker at once.
            async with self._poll_semaphore:
                await self.device.update_state()
            self._error_count = 0
            return copy.deepcopy(self.device.raw_properties)

        except asyncio.TimeoutError as error:
            self._error_count += 1
            if self._error_count >= MAX_ERRORS:
                _LOGGER.warning(
                    "Cloud device %s is unavailable after %d timeouts",
                    self.name,
                    self._error_count,
                )
                raise UpdateFailed(
                    f"Cloud device {self.name} is unavailable, timeout"
                ) from error
            # Return last known state if within error threshold
            return copy.deepcopy(self.device.raw_properties)

        except Exception as error:
            if _is_mqtt_disconnected(error):
                _LOGGER.warning(
                    "MQTT disconnected while updating %s — triggering reconnect",
                    self.name,
                )
                reconnected = await _try_reconnect(self.hass, self.config_entry)
                if reconnected:
                    try:
                        await self.device.update_state()
                        self._error_count = 0
                        return copy.deepcopy(self.device.raw_properties)
                    except Exception as retry_error:
                        _LOGGER.warning(
                            "State update failed after reconnect for %s: %s",
                            self.name,
                            retry_error,
                        )

            self._error_count += 1
            _LOGGER.exception("Error updating cloud device %s: %s", self.name, error)
            if self._error_count >= MAX_ERRORS:
                raise UpdateFailed(
                    f"Cloud device {self.name} failed to update"
                ) from error
            return copy.deepcopy(self.device.raw_properties)

    async def push_state_update(self):
        """Send state updates to the cloud device."""
        try:
            return await self.device.push_state_update()
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Timeout sending state update to cloud device: %s", self.name
            )
        except Exception as error:
            if _is_mqtt_disconnected(error):
                _LOGGER.warning(
                    "MQTT disconnected while pushing state to %s — triggering reconnect",
                    self.name,
                )
                reconnected = await _try_reconnect(self.hass, self.config_entry)
                if reconnected:
                    try:
                        return await self.device.push_state_update()
                    except Exception as retry_error:
                        _LOGGER.warning(
                            "Push state failed after reconnect for %s: %s",
                            self.name,
                            retry_error,
                        )
                        return
            _LOGGER.exception(
                "Error sending state update to cloud device %s: %s", self.name, error
            )


class CloudDiscoveryService:
    """Cloud discovery service for Gree devices.

    Designed for fleets ranging from a handful of devices up to several
    hundred (e.g. many branches under one Gree+ account):
      - Devices are processed in configurable batches, with a pause between
        batches to avoid hammering Gree's MQTT/cloud servers.
      - A semaphore bounds how many devices are actively being bound/
        refreshed at once, regardless of batch size.
      - Each device gets its own retry loop with a delay between attempts.
      - One device failing (even after retries) never stops the others —
        exceptions are always contained per-device.
    """

    def __init__(
        self, hass: HomeAssistant, entry: GreeCloudConfigEntry, api: GreeCloudApi
    ) -> None:
        """Initialize cloud discovery service."""
        self.hass = hass
        self.entry = entry
        self.api = api

    def _option(self, key: str, default: Any) -> Any:
        return self.entry.options.get(key, default)

    async def discover_devices(
        self, mqtt_client: GreeMqttClient
    ) -> list[CloudDeviceDataUpdateCoordinator]:
        """Discover, bind and do the first refresh for every cloud device."""
        from .const import (
            CONF_BATCH_DELAY,
            CONF_BATCH_SIZE,
            CONF_CONCURRENCY_LIMIT,
            CONF_DEVICE_TIMEOUT,
            CONF_RETRY_ATTEMPTS,
            CONF_RETRY_DELAY,
            DEFAULT_BATCH_DELAY,
            DEFAULT_BATCH_SIZE,
            DEFAULT_CONCURRENCY_LIMIT,
            DEFAULT_DEVICE_TIMEOUT,
            DEFAULT_RETRY_ATTEMPTS,
            DEFAULT_RETRY_DELAY,
        )

        batch_size = self._option(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE)
        concurrency_limit = self._option(CONF_CONCURRENCY_LIMIT, DEFAULT_CONCURRENCY_LIMIT)
        batch_delay = self._option(CONF_BATCH_DELAY, DEFAULT_BATCH_DELAY)
        retry_attempts = self._option(CONF_RETRY_ATTEMPTS, DEFAULT_RETRY_ATTEMPTS)
        retry_delay = self._option(CONF_RETRY_DELAY, DEFAULT_RETRY_DELAY)
        device_timeout = self._option(CONF_DEVICE_TIMEOUT, DEFAULT_DEVICE_TIMEOUT)

        coordinators: list[CloudDeviceDataUpdateCoordinator] = []

        try:
            _LOGGER.debug("Fetching device list from Gree Cloud")
            cloud_devices = await self.api.get_all_devices()
        except Exception:
            _LOGGER.exception("Failed to fetch device list from Gree Cloud")
            return coordinators

        total = len(cloud_devices)
        _LOGGER.info(
            "Gree Cloud: found %d device(s). Discovering in batches of %d "
            "(max %d concurrent, %d retr%s per device, %.1fs between batches)",
            total,
            batch_size,
            concurrency_limit,
            retry_attempts,
            "y" if retry_attempts == 1 else "ies",
            batch_delay,
        )

        semaphore = asyncio.Semaphore(max(1, concurrency_limit))
        stats = {"bound": 0, "failed": 0, "retried": 0}

        async def setup_one(
            cloud_dev_info: CloudDeviceInfo, position: int
        ) -> CloudDeviceDataUpdateCoordinator | None:
            """Bind one device and get its first state, with retries."""
            async with semaphore:
                last_err: Exception | None = None
                for attempt in range(1, max(1, retry_attempts) + 1):
                    try:
                        device_info = DeviceInfo(
                            ip="0.0.0.0",  # Not used for cloud devices
                            port=0,  # Not used for cloud devices
                            mac=cloud_dev_info.mac,
                            name=cloud_dev_info.name,
                        )
                        device = HWHPAwareCloudDevice(
                            mqtt_client=mqtt_client,
                            device_info=device_info,
                            device_key=cloud_dev_info.key,
                            cipher_version=1,
                        )

                        await asyncio.wait_for(device.bind(), timeout=device_timeout)

                        coordinator = CloudDeviceDataUpdateCoordinator(
                            self.hass, self.entry, device
                        )
                        # NOTE: we deliberately use async_refresh(), not
                        # async_config_entry_first_refresh(). The latter is
                        # only valid while the config entry is still in
                        # SETUP_IN_PROGRESS state, but discovery now runs as
                        # a background task *after* setup has already
                        # finished (entry state LOADED) so devices can come
                        # online progressively without blocking bootstrap.
                        # async_refresh() does the same "first update" work
                        # without that restriction.
                        await asyncio.wait_for(
                            coordinator.async_refresh(),
                            timeout=device_timeout,
                        )

                        stats["bound"] += 1
                        _LOGGER.debug(
                            "[%d/%d] Bound cloud device: %s (MAC: %s)%s",
                            position,
                            total,
                            cloud_dev_info.name,
                            cloud_dev_info.mac,
                            f" (attempt {attempt})" if attempt > 1 else "",
                        )

                        async_dispatcher_send(
                            self.hass, DISPATCH_DEVICE_DISCOVERED, coordinator
                        )
                        return coordinator

                    except Exception as err:  # noqa: BLE001 - contained per device
                        last_err = err
                        if attempt < retry_attempts:
                            stats["retried"] += 1
                            _LOGGER.debug(
                                "[%d/%d] Attempt %d/%d failed for %s (MAC: %s): "
                                "%s — retrying in %.1fs",
                                position,
                                total,
                                attempt,
                                retry_attempts,
                                cloud_dev_info.name,
                                cloud_dev_info.mac,
                                err,
                                retry_delay,
                            )
                            await asyncio.sleep(retry_delay)
                            continue

                stats["failed"] += 1
                _LOGGER.warning(
                    "[%d/%d] Giving up on device %s (MAC: %s) after %d "
                    "attempt(s): %s",
                    position,
                    total,
                    cloud_dev_info.name,
                    cloud_dev_info.mac,
                    retry_attempts,
                    last_err,
                )
                return None

        for batch_start in range(0, total, max(1, batch_size)):
            batch = cloud_devices[batch_start : batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            _LOGGER.info(
                "Gree Cloud: processing batch %d/%d (devices %d-%d of %d)",
                batch_num,
                total_batches,
                batch_start + 1,
                batch_start + len(batch),
                total,
            )

            results = await asyncio.gather(
                *(
                    setup_one(dev, batch_start + i + 1)
                    for i, dev in enumerate(batch)
                ),
                return_exceptions=True,
            )

            for dev, result in zip(batch, results):
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    _LOGGER.error(
                        "Unexpected error setting up device %s (MAC: %s): %s",
                        dev.name,
                        dev.mac,
                        result,
                    )
                elif result is not None:
                    coordinators.append(result)

            if batch_start + batch_size < total and batch_delay:
                await asyncio.sleep(batch_delay)

        _LOGGER.info(
            "Gree Cloud: discovery complete — %d bound, %d failed, %d retried "
            "(out of %d total device(s))",
            stats["bound"],
            stats["failed"],
            stats["retried"],
            total,
        )

        return coordinators
