"""Shared stateful fake Redis for report-back tests.

Models the SET / LIST / KV / pipeline ops the report-back path uses so the
outbox executor's dispatch/clear flow is exercised for real.
"""

from __future__ import annotations

import json

from src.server.services.report_back.flash import keys, pointer, reserve


class FakePipeline:
    """Queues client ops and replays them against the fake client on execute.

    Mirrors redis-py's async pipeline shape: command methods are synchronous
    (queue + return self), ``execute`` is awaited and runs them in order.
    """

    def __init__(self, client: "FakeClient") -> None:
        self._client = client
        self._ops: list = []

    def __getattr__(self, name):
        def _queue(*args, **kwargs) -> "FakePipeline":
            self._ops.append((name, args, kwargs))
            return self

        return _queue

    async def execute(self) -> list:
        results = []
        for name, args, kwargs in self._ops:
            results.append(await getattr(self._client, name)(*args, **kwargs))
        self._ops.clear()
        return results


class FakeClient:
    def __init__(self) -> None:
        self.sets: dict[str, set] = {}
        self.lists: dict[str, list] = {}
        # Shared with FakeCache.kv so raw-client DELETE (pipeline) and the
        # wrapper's get/set/delete address one keyspace, as real Redis does.
        self.kv: dict[str, object] = {}
        self.hashes: dict[str, dict] = {}
        self.published: list[tuple[str, str]] = []
        # Last TTL set per key, so tests can assert EXPIRE was issued.
        self.ttls: dict[str, int] = {}

    async def sismember(self, key, member) -> bool:
        return member in self.sets.get(key, set())

    async def sadd(self, key, member) -> int:
        s = self.sets.setdefault(key, set())
        if member in s:
            return 0
        s.add(member)
        return 1

    async def srem(self, key, member) -> None:
        self.sets.get(key, set()).discard(member)

    async def scard(self, key) -> int:
        return len(self.sets.get(key, set()))

    async def smembers(self, key) -> set:
        return set(self.sets.get(key, set()))

    async def rpush(self, key, value) -> None:
        self.lists.setdefault(key, []).append(value)

    async def lindex(self, key, index):
        lst = self.lists.get(key, [])
        return lst[index] if -len(lst) <= index < len(lst) else None

    async def lrem(self, key, count, value) -> int:
        lst = self.lists.get(key, [])
        if count == 0:
            kept = [x for x in lst if x != value]
            self.lists[key] = kept
            return len(lst) - len(kept)
        removed = 0
        out = []
        for x in lst:
            if x == value and removed < count:
                removed += 1
                continue
            out.append(x)
        self.lists[key] = out
        return removed

    async def llen(self, key) -> int:
        return len(self.lists.get(key, []))

    async def lpush(self, key, value) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, stop) -> None:
        lst = self.lists.get(key, [])
        self.lists[key] = lst[start : None if stop == -1 else stop + 1]

    async def lrange(self, key, start, stop) -> list:
        lst = self.lists.get(key, [])
        return list(lst[start : None if stop == -1 else stop + 1])

    async def mget(self, keys) -> list:
        # The raw client hands back serialized values; the wrapper's kv holds
        # parsed ones, so re-serialize dicts the way prod Redis would.
        return [
            json.dumps(v) if isinstance(v, dict) else v
            for v in (self.kv.get(k) for k in keys)
        ]

    async def hset(self, key, mapping=None) -> int:
        h = self.hashes.setdefault(key, {})
        h.update({str(k): str(v) for k, v in (mapping or {}).items()})
        return len(mapping or {})

    async def hgetall(self, key) -> dict:
        # The real client runs decode_responses=False → bytes in and out.
        return {
            k.encode(): str(v).encode()
            for k, v in self.hashes.get(key, {}).items()
        }

    async def expire(self, key, ttl) -> None:
        self.ttls[key] = ttl

    async def delete(self, *keys) -> None:
        for key in keys:
            self.sets.pop(key, None)
            self.lists.pop(key, None)
            self.kv.pop(key, None)
            self.hashes.pop(key, None)

    async def scan_iter(self, match=None):
        import fnmatch

        for key in list(set(self.kv) | set(self.sets) | set(self.lists)):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    async def publish(self, channel, message) -> None:
        self.published.append((channel, message))

    def _decoded(self, key):
        current = self.kv.get(key)
        if isinstance(current, (str, bytes)):
            try:
                return json.loads(current)
            except (TypeError, ValueError):
                return None
        return current

    async def eval(self, script, numkeys, *args):
        """Emulates the report-back Lua scripts by identity."""
        keys, argv = args[:numkeys], args[numkeys:]
        if script is reserve.ADMISSION_GATE_LUA:
            receipt_key, origin_key = keys
            gen, run_id = argv
            if gen in self.sets.get(receipt_key, set()):
                return 0
            origin_blob = self._decoded(origin_key)
            if (
                isinstance(origin_blob, dict)
                and origin_blob.get("dispatch_gen") == gen
            ):
                # KEEPTTL: value replaced, self.ttls entry untouched.
                if origin_blob.get("admitted_gen") != gen:
                    origin_blob["admitted_gen"] = gen
                    origin_blob["admitted_run"] = run_id
                pending = origin_blob.get("pending_runs")
                if not isinstance(pending, dict):
                    pending = {}
                    origin_blob["pending_runs"] = pending
                pending[run_id] = True
                self.kv[origin_key] = origin_blob
            return 1
        if script is reserve.ADMISSION_RETRACT_LUA:
            (origin_key,) = keys
            gen, run_id = argv
            origin_blob = self._decoded(origin_key)
            if not isinstance(origin_blob, dict):
                return 0
            if origin_blob.get("dispatch_gen") != gen:
                return 0
            dirty = False
            if (
                origin_blob.get("admitted_gen") == gen
                and origin_blob.get("admitted_run") == run_id
            ):
                origin_blob.pop("admitted_gen", None)
                origin_blob.pop("admitted_run", None)
                dirty = True
            pending = origin_blob.get("pending_runs")
            if isinstance(pending, dict) and pending.pop(run_id, None):
                dirty = True
            if not dirty:
                return 0
            self.kv[origin_key] = origin_blob
            return 1
        if script is pointer.GATED_POINTER_SET_LUA:
            watch_key, run_key = keys
            ptc_id, run_id, value, ttl, request_key, gen = argv
            if ptc_id not in self.sets.get(watch_key, set()):
                return 0
            current = self._decoded(run_key)
            if self.kv.get(run_key) is not None and not isinstance(current, dict):
                return 0
            if isinstance(current, dict) and current.get("run_id") != run_id:
                # A different run owns the pointer: replace only another
                # job's pointer (request-key scoped; old-format pointers
                # fall back to the generation rule).
                if isinstance(current.get("request_key"), str):
                    if request_key == "" or current["request_key"] == request_key:
                        return 0
                else:
                    if gen == "":
                        return 0
                    if (
                        isinstance(current.get("dispatch_gen"), str)
                        and current.get("dispatch_gen") == gen
                    ):
                        return 0
            # Store decoded, matching how the wrapper's kv holds parsed values.
            self.kv[run_key] = json.loads(value)
            self.ttls[run_key] = int(ttl)
            return 1
        if script is pointer.CLAIM_POINTER_LUA:
            watch_key, run_key = keys
            ptc_id, value, ttl, request_key, gen = argv
            current = self._decoded(run_key)
            if isinstance(current, dict) and isinstance(current.get("run_id"), str):
                # The real script returns the RAW pointer bytes; the fake's kv
                # may hold parsed dicts, so re-serialize like real Redis would.
                raw = self.kv.get(run_key)
                raw = raw if isinstance(raw, str) else json.dumps(current)
                if isinstance(current.get("request_key"), str):
                    if request_key == "" or current["request_key"] == request_key:
                        return [0, raw]
                elif (request_key == "" and gen == "") or (
                    gen != ""
                    and isinstance(current.get("dispatch_gen"), str)
                    and current.get("dispatch_gen") == gen
                ):
                    return [0, raw]
            if ptc_id not in self.sets.get(watch_key, set()):
                return [2, ""]
            self.kv[run_key] = json.loads(value)
            self.ttls[run_key] = int(ttl)
            return [1, ""]
        if script is pointer.POINTER_TAKEOVER_LUA:
            watch_key, run_key = keys
            ptc_id, expected, value, ttl = argv
            raw = self.kv.get(run_key)
            if raw is None:
                return 0
            raw = raw if isinstance(raw, str) else json.dumps(raw)
            if raw != expected:
                return 0
            if ptc_id not in self.sets.get(watch_key, set()):
                return 2
            self.kv[run_key] = json.loads(value)
            self.ttls[run_key] = int(ttl)
            return 1
        if script is reserve.REAP_ORPHANS_LUA:
            set_key, origin_keys = keys[0], keys[1:]
            removed = 0
            for origin_key, member in zip(origin_keys, argv):
                if origin_key not in self.kv and member in self.sets.get(
                    set_key, set()
                ):
                    self.sets[set_key].discard(member)
                    removed += 1
            return removed
        if script is reserve.ORPHAN_RESOLVE_LUA:
            (
                origin_key,
                watch_key,
                user_key,
                run_key,
                receipt_key,
                done_key,
            ) = keys
            ptc_id, fencer_gen, receipt_ttl, done_max, done_ttl, job_gen = argv
            if self.kv.get(origin_key) is None:
                return [0, "origin_gone"]
            origin_blob = self._decoded(origin_key)
            if not isinstance(origin_blob, dict):
                return [0, "origin_unreadable"]
            if origin_blob.get("dispatch_gen") != fencer_gen:
                return [0, "origin_moved"]
            if origin_blob.get("admitted_gen") == fencer_gen:
                return [0, "admitted"]
            pending = origin_blob.get("pending_runs")
            if isinstance(pending, dict) and pending:
                return [0, "admitted"]
            surrogate = job_gen != "" and job_gen in (
                origin_blob.get("prev_gen"),
                origin_blob.get("owner_gen"),
            )
            self.sets.setdefault(receipt_key, set()).add(fencer_gen)
            self.ttls[receipt_key] = int(receipt_ttl)
            watch_removed = 0
            user_removed = 0
            if watch_key and (
                origin_blob.get("owns_watch") is True or surrogate
            ):
                if ptc_id in self.sets.get(watch_key, set()):
                    self.sets[watch_key].discard(ptc_id)
                    watch_removed = 1
            if user_key and (
                origin_blob.get("owns_user") is True or surrogate
            ):
                if ptc_id in self.sets.get(user_key, set()):
                    self.sets[user_key].discard(ptc_id)
                    user_removed = 1
            ptr = ""
            if run_key and self.kv.get(run_key) is not None:
                current = self._decoded(run_key)
                if (
                    not surrogate
                    and isinstance(current, dict)
                    and isinstance(current.get("dispatch_gen"), str)
                    and current["dispatch_gen"] != fencer_gen
                ):
                    ptr = ""  # foreign generation's pointer — spared
                else:
                    raw = self.kv.pop(run_key)
                    ptr = raw if isinstance(raw, str) else json.dumps(raw)
                    run_id = (
                        current.get("run_id")
                        if isinstance(current, dict)
                        else None
                    )
                    if isinstance(run_id, str) and done_key:
                        lst = self.lists.setdefault(done_key, [])
                        self.lists[done_key] = [
                            run_id,
                            *[x for x in lst if x != run_id],
                        ][: int(done_max)]
                        self.ttls[done_key] = int(done_ttl)
            return [1, ptr, watch_removed, user_removed]
        if script is pointer.GATED_TEARDOWN_LUA:
            origin_key, run_key, watch_key, user_key, tomb_key = keys
            expected_gen, ptc_id, tomb_ttl, refuse_if_pointer = argv
            # Off-chain caller + live run pointer: refuse everything — only
            # that admission's serialized lifecycle may drain the pair.
            if refuse_if_pointer == "1" and run_key and run_key in self.kv:
                return [0, ""]
            current = self._decoded(origin_key)
            if isinstance(current, dict) and isinstance(
                current.get("dispatch_gen"), str
            ):
                # A generated origin falls only to a caller presenting ITS gen.
                if expected_gen == "" or current["dispatch_gen"] != expected_gen:
                    # Every fenced-out teardown records its identity in the
                    # tombstone SET ('__legacy__' for gen-less callers) so a
                    # later rollback of the fencing provisional generation
                    # honors this clear instead of restoring its target.
                    self.sets.setdefault(tomb_key, set()).add(
                        expected_gen or "__legacy__"
                    )
                    self.ttls[tomb_key] = int(tomb_ttl)
                    return [0, current["dispatch_gen"]]
            self.kv.pop(origin_key, None)
            self.sets.pop(tomb_key, None)
            if run_key:
                self.kv.pop(run_key, None)
            if watch_key:
                self.sets.get(watch_key, set()).discard(ptc_id)
            if user_key:
                self.sets.get(user_key, set()).discard(ptc_id)
            return 1
        if script is pointer.POINTER_COMPARE_DELETE_LUA:
            (run_key,), (run_id,) = keys, argv
            current = self._decoded(run_key)
            if current is None and self.kv.get(run_key) is not None:
                return 0
            if isinstance(current, dict) and current.get("run_id") == run_id:
                self.kv.pop(run_key, None)
                return 1
            return 0
        if script is reserve.RESERVE_LUA:
            watch_key, user_key, origin_key = keys
            ptc_id, flash_id, max_flash, max_user, ttl, origin_json = argv
            prev = self._decoded(origin_key)
            if isinstance(prev, dict) and prev.get("flash_thread_id") not in (
                None,
                flash_id,
            ):
                return ["cross", 0, 0, ""]
            in_watch = ptc_id in self.sets.get(watch_key, set())
            in_user = ptc_id in self.sets.get(user_key, set())
            if not in_watch and len(self.sets.get(watch_key, set())) >= int(max_flash):
                return ["cap_flash", 0, 0, ""]
            if not in_user and len(self.sets.get(user_key, set())) >= int(max_user):
                return ["cap_user", 0, 0, ""]
            if not in_watch:
                self.sets.setdefault(watch_key, set()).add(ptc_id)
            self.ttls[watch_key] = int(ttl)
            if not in_user:
                self.sets.setdefault(user_key, set()).add(ptc_id)
            self.ttls[user_key] = int(ttl)
            og = json.loads(origin_json)
            og["owns_watch"] = not in_watch
            og["owns_user"] = not in_user
            if isinstance(prev, dict) and isinstance(
                prev.get("dispatch_gen"), str
            ):
                og["prev_gen"] = prev["dispatch_gen"]
                if (
                    prev.get("admitted_gen") == prev["dispatch_gen"]
                    or prev.get("owns_watch") is True
                    or prev.get("owns_user") is True
                    or "owns_watch" not in prev
                ):
                    og["owner_gen"] = prev["dispatch_gen"]
                elif isinstance(prev.get("owner_gen"), str):
                    og["owner_gen"] = prev["owner_gen"]
            self.kv[origin_key] = og
            self.ttls[origin_key] = int(ttl)
            prev_raw = json.dumps(prev) if isinstance(prev, dict) else ""
            return ["ok", 0 if in_watch else 1, 0 if in_user else 1, prev_raw]
        if script is reserve.ROLLBACK_RESERVE_LUA:
            watch_key, user_key, origin_key, tomb_key, run_key = keys
            ptc_id, minted_gen, prev_json, added_watch, added_user, ttl = argv
            current = self._decoded(origin_key)
            if not (
                isinstance(current, dict)
                and current.get("dispatch_gen") == minted_gen
            ):
                return 0
            if prev_json:
                tombs = self.sets.get(tomb_key, set())
                prev = json.loads(prev_json)
                if isinstance(prev, dict) and isinstance(
                    prev.get("dispatch_gen"), str
                ):
                    dead = prev["dispatch_gen"] in tombs
                else:
                    # Legacy predecessor: ANY fenced clear would have been
                    # authorized against a gen-less origin.
                    dead = bool(tombs)
                if dead:
                    # The stashed predecessor's teardown was fenced out while
                    # our provisional gen held the origin — honor it: clear
                    # the whole pair instead of resurrecting torn-down state,
                    # handing back the consumed run pointer for recording.
                    ptr = ""
                    if run_key:
                        pv = self.kv.pop(run_key, None)
                        if pv is not None:
                            ptr = json.dumps(pv) if isinstance(pv, dict) else pv
                    self.kv.pop(origin_key, None)
                    self.sets.pop(tomb_key, None)
                    self.sets.get(watch_key, set()).discard(ptc_id)
                    self.sets.get(user_key, set()).discard(ptc_id)
                    return [2, ptr]
                self.kv[origin_key] = prev
                self.ttls[origin_key] = int(ttl)
            else:
                self.kv.pop(origin_key, None)
            if added_watch == "1":
                self.sets.get(watch_key, set()).discard(ptc_id)
            if added_user == "1":
                self.sets.get(user_key, set()).discard(ptc_id)
            return 1
        raise AssertionError(f"unknown Lua script: {script[:60]!r}")

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)


class FakeCache:
    def __init__(self) -> None:
        self.enabled = True
        self.client = FakeClient()
        # One keyspace: the wrapper's string KV is the client's kv dict.
        self.kv: dict[str, object] = self.client.kv

    async def get(self, key):
        return self.kv.get(key)

    async def get_strict(self, key):
        # Fake transport never fails, so strict == plain get here.
        return self.kv.get(key)

    async def set(self, key, value, ttl=None) -> bool:
        # Mirror RedisCache.set's True-on-success: reserve()'s fail-closed
        # origin write treats a falsy return as a dispatch failure.
        self.kv[key] = value
        return True

    async def delete(self, key) -> None:
        self.kv.pop(key, None)


def origin(ptc: str, flash: str = "flash-1", user: str = "u-1") -> dict:
    return {
        "origin": "flash",
        "report_back": True,
        "flash_thread_id": flash,
        "flash_workspace_id": "fws-1",
        "ptc_thread_id": ptc,
        "ptc_workspace_id": f"ws-{ptc}",
        "user_id": user,
    }


def seed_dispatched(cache: FakeCache, flash: str, ptcs: list[str], user: str = "u-1") -> None:
    """Mirror what reservation + origin recording leave behind for each FRESH
    dispatch (the reserve script created both memberships, so the origin
    carries ownership of them)."""
    for ptc in ptcs:
        cache.client.sets.setdefault(keys.flash_watch_key(flash), set()).add(ptc)
        cache.client.sets.setdefault(keys.flash_user_pending_key(user), set()).add(ptc)
        cache.kv[keys.ptc_origin_key(ptc)] = {
            **origin(ptc, flash, user),
            "owns_watch": True,
            "owns_user": True,
        }
