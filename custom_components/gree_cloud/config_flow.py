"""Config flow for Gree Climate Cloud integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_BATCH_DELAY,
    CONF_BATCH_SIZE,
    CONF_CONCURRENCY_LIMIT,
    CONF_DEVICE_TIMEOUT,
    CONF_POLL_CONCURRENCY_LIMIT,
    CONF_RETRY_ATTEMPTS,
    CONF_RETRY_DELAY,
    CONF_SERVER,
    DEFAULT_BATCH_DELAY,
    DEFAULT_BATCH_SIZE,
    DEFAULT_CONCURRENCY_LIMIT,
    DEFAULT_DEVICE_TIMEOUT,
    DEFAULT_POLL_CONCURRENCY_LIMIT,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY,
    DOMAIN,
    GREE_CLOUD_SERVERS,
)

_LOGGER = logging.getLogger(__name__)


class GreeCloudConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Gree Climate Cloud."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Check if already configured
            await self.async_set_unique_id(user_input[CONF_USERNAME])
            self._abort_if_unique_id_configured()

            # Validate credentials by attempting to login
            try:
                from greeclimate.cloud_api import GreeCloudApi

                api = GreeCloudApi.for_server(
                    user_input[CONF_SERVER],
                    user_input[CONF_USERNAME],
                    user_input[CONF_PASSWORD],
                )

                # Try to login to validate credentials
                await api.login()
                await api.close()

                # Create the config entry
                return self.async_create_entry(
                    title=f"Gree Cloud ({user_input[CONF_USERNAME]})",
                    data=user_input,
                )

            except ValueError as err:
                _LOGGER.error("Invalid server selection: %s", err)
                errors["base"] = "invalid_auth"
            except Exception as err:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected error during Gree Cloud login: %s", err)
                if "401" in str(err) or "auth" in str(err).lower():
                    errors["base"] = "invalid_auth"
                else:
                    errors["base"] = "cannot_connect"

        # Show the configuration form
        data_schema = vol.Schema(
            {
                vol.Required(CONF_SERVER, default="Europe"): vol.In(
                    list(GREE_CLOUD_SERVERS.keys())
                ),
                vol.Required(CONF_USERNAME): str,
                vol.Required(CONF_PASSWORD): str,
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> GreeCloudOptionsFlow:
        """Get the options flow for this handler."""
        return GreeCloudOptionsFlow()


class GreeCloudOptionsFlow(config_entries.OptionsFlow):
    """Options flow for tuning large-fleet discovery behaviour.

    Useful when a single Gree+ account has many devices (dozens to hundreds,
    e.g. spread across many branches): lets you control how many devices are
    bound at once, how much the discovery pauses between batches to ease
    load on Gree's MQTT/cloud servers, and how retries are handled.
    """

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current = self.config_entry.options
        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_BATCH_SIZE,
                    default=current.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=100)),
                vol.Required(
                    CONF_CONCURRENCY_LIMIT,
                    default=current.get(
                        CONF_CONCURRENCY_LIMIT, DEFAULT_CONCURRENCY_LIMIT
                    ),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=50)),
                vol.Required(
                    CONF_POLL_CONCURRENCY_LIMIT,
                    default=current.get(
                        CONF_POLL_CONCURRENCY_LIMIT, DEFAULT_POLL_CONCURRENCY_LIMIT
                    ),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=50)),
                vol.Required(
                    CONF_BATCH_DELAY,
                    default=current.get(CONF_BATCH_DELAY, DEFAULT_BATCH_DELAY),
                ): vol.All(vol.Coerce(float), vol.Range(min=0, max=120)),
                vol.Required(
                    CONF_RETRY_ATTEMPTS,
                    default=current.get(CONF_RETRY_ATTEMPTS, DEFAULT_RETRY_ATTEMPTS),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=10)),
                vol.Required(
                    CONF_RETRY_DELAY,
                    default=current.get(CONF_RETRY_DELAY, DEFAULT_RETRY_DELAY),
                ): vol.All(vol.Coerce(float), vol.Range(min=0, max=120)),
                vol.Required(
                    CONF_DEVICE_TIMEOUT,
                    default=current.get(CONF_DEVICE_TIMEOUT, DEFAULT_DEVICE_TIMEOUT),
                ): vol.All(vol.Coerce(int), vol.Range(min=5, max=300)),
            }
        )

        return self.async_show_form(step_id="init", data_schema=data_schema)
