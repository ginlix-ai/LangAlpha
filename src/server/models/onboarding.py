"""The onboarding progress record (``onboarding_progress``, migration 135)."""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool

Models = Literal["claude_code", "subscription_or_key", "langalpha_models"]
RunsOn = Literal["cloud", "own_computer"]
ThisMac = Literal["paired", "declined", "unavailable"]
SetupOn = Literal["browser", "mac_pending", "app"]
SetupStep = Literal["computer", "model", "default"]

# The stamp in ``user_preferences.other_preference`` set when the account's
# hosted onboarding finished, read from preferences without the cache.
ACCOUNT_SETUP_KEY = "account_setup_completed_at"


class OnboardingProgressResponse(BaseModel):
    models: Optional[Models] = None
    runs_on: Optional[RunsOn] = None
    plan_step_done_at: Optional[datetime] = None
    this_mac: Optional[ThisMac] = None
    setup_on: Optional[SetupOn] = None
    setup_step: Optional[SetupStep] = None
    setup_completed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    account_setup_completed_at: Optional[datetime] = Field(
        None,
        description="When the account's hosted onboarding finished",
    )


class OnboardingProgressPatch(BaseModel):
    """Only the keys sent are written, so an explicit null is told apart from
    an absent key (``model_fields_set``): null clears, absent keeps.

    ``plan_step_done`` and ``setup_completed`` stamp their time the first
    time they are true and keep that stamp after; false or null clears it.
    Several onboarding surfaces write the record independently, so their
    answer to "when" must not move each time one of them says so again.
    """

    model_config = ConfigDict(extra="forbid")

    models: Optional[Models] = None
    runs_on: Optional[RunsOn] = None
    this_mac: Optional[ThisMac] = None
    setup_on: Optional[SetupOn] = None
    setup_step: Optional[SetupStep] = None
    plan_step_done: Optional[StrictBool] = None
    setup_completed: Optional[StrictBool] = None
