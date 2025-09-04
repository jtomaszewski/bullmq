--[[
  Function to remove deduplication key if needed
  when a job is moved to completed or failed states.
]]

local function removeDeduplicationKeyIfNeededOnFinalization(prefixKey,
  deduplicationId, jobId)
  if deduplicationId then
    local deduplicationKey = prefixKey .. "de:" .. deduplicationId
    local requeueKey = prefixKey .. "re:" .. deduplicationId
    local pttl = rcall("PTTL", deduplicationKey)

    if pttl == 0 then
      rcall("DEL", requeueKey)
      return rcall("DEL", deduplicationKey)
    end

    if pttl == -1 then
      local currentJobId = rcall('GET', deduplicationKey)
      if currentJobId and currentJobId == jobId then
        -- Check for pending requeue
        local requeueJobId = rcall('GET', requeueKey)
        if requeueJobId then
          -- There's a pending requeue, update deduplication key to point to the requeued job
          -- and clean up the requeue key
          rcall("SET", deduplicationKey, requeueJobId)
          rcall("DEL", requeueKey)
          return requeueJobId
        else
          -- No pending requeue, clean up the deduplication key
          return rcall("DEL", deduplicationKey)
        end
      end
    end
  end
end
