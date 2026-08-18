PUBLISH_OUTBOX_EVENT = """
local dedupe_key = KEYS[1]
local stream_key = KEYS[2]

local event_id = ARGV[1]
local event_type = ARGV[2]
local owner_id = ARGV[3]
local endpoint_id = ARGV[4]
local current_status = ARGV[5]
local endpoint_url = ARGV[6]
local latency_ms = ARGV[7]

local created = redis.call(
    "SET",
    dedupe_key,
    "1",
    "NX"
)

if not created then
    return {"duplicate"}
end

local ok, stream_id = pcall(
    redis.call,
    "XADD",
    stream_key,
    "*",

    "outbox_event_id", event_id,
    "event_type", event_type,
    "owner_id", owner_id,
    "endpoint_id", endpoint_id,
    "current_status", current_status,
    "endpoint_url", endpoint_url,
    "latency_ms", latency_ms
)

if not ok then
    redis.call("DEL", dedupe_key)
    return {"xadd_failed"}
end

return {"published", stream_id}
""" 