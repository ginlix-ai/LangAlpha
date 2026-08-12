"""Provider adapters for the web provider protocol.

Each module owns one provider: its API wrapper (raw httpx, key read lazily
from env), its search tool builder (provider-native schema), and/or its
fetch adapter (normalized FetchRequest/FetchResponse).
"""
