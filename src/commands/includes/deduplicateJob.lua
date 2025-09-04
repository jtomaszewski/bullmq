--[[
  Function to debounce a job.
]]
-- Includes
--- @include "removeJobKeys"
local function deduplicateJob(deduplicationOpts, jobId, delayedKey, deduplicationKey, eventsKey, maxEvents,
    prefix, activeKey)
    local deduplicationId = deduplicationOpts and deduplicationOpts['id']
    if deduplicationId then
        local requeueIfActive = deduplicationOpts['requeueIfActive']
        local requeueKey = prefix .. "re:" .. deduplicationId
        
        -- Check if requeue mode is enabled and handle it first
        if requeueIfActive then
            local currentJobId = rcall('GET', deduplicationKey)
            if currentJobId then
                -- Check if current job is active (only if activeKey is provided)
                if activeKey and rcall("LPOS", activeKey, currentJobId) then
                    -- Set pending-requeue flag with the job ID that will be created
                    rcall('SET', requeueKey, jobId)
                    -- Return a special value indicating requeue mode - don't return the current job ID
                    -- This allows the new job to be created but not added to the wait queue
                    return "REQUEUE:" .. currentJobId
                else
                    -- Current job is not active, check if it's waiting/delayed
                    local currentJobKey = prefix .. currentJobId
                    if rcall("EXISTS", currentJobKey) == 1 then
                        -- Job exists but is not active
                        -- If replace option is enabled, let it be handled by replace logic below
                        local ttl = deduplicationOpts['ttl']
                        if deduplicationOpts['replace'] and ttl and ttl > 0 then
                            -- Let replace logic handle this case
                        else
                            -- Normal deduplication for requeueIfActive
                            if ttl and deduplicationOpts['extend'] then
                                rcall('SET', deduplicationKey, currentJobId, 'PX', ttl)
                            end
                            rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "deduplicated", "jobId",
                                currentJobId, "deduplicationId", deduplicationId, "deduplicatedJobId", jobId)
                            return currentJobId
                        end
                    end
                    -- Current job doesn't exist (completed/failed), allow new job to proceed with normal deduplication logic
                end
            end
        end
        
        local ttl = deduplicationOpts['ttl']
        if deduplicationOpts['replace'] and ttl and ttl > 0 then
            local currentDebounceJobId = rcall('GET', deduplicationKey)
            if currentDebounceJobId then
                if rcall("ZREM", delayedKey, currentDebounceJobId) > 0 then
                    removeJobKeys(prefix .. currentDebounceJobId)
                    rcall("XADD", eventsKey, "*", "event", "removed", "jobId", currentDebounceJobId,
                        "prev", "delayed")

                    if deduplicationOpts['extend'] then
                        rcall('SET', deduplicationKey, jobId, 'PX', ttl)
                    else
                        rcall('SET', deduplicationKey, jobId, 'KEEPTTL')
                    end

                    rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "deduplicated", "jobId",
                        jobId, "deduplicationId", deduplicationId, "deduplicatedJobId", currentDebounceJobId)
                    return
                else
                    return currentDebounceJobId
                end
            else
                rcall('SET', deduplicationKey, jobId, 'PX', ttl)
                return
            end
        else
            local ttl = deduplicationOpts['ttl']
            local deduplicationKeyExists
            if ttl then
                if deduplicationOpts['extend'] then
                    local currentDebounceJobId = rcall('GET', deduplicationKey)
                    if currentDebounceJobId then
                        rcall('SET', deduplicationKey, currentDebounceJobId, 'PX', ttl)
                        rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "debounced",
                            "jobId", currentDebounceJobId, "debounceId", deduplicationId)
                        rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "deduplicated", "jobId",
                            currentDebounceJobId, "deduplicationId", deduplicationId, "deduplicatedJobId", jobId)
                        return currentDebounceJobId
                    else
                        rcall('SET', deduplicationKey, jobId, 'PX', ttl)
                        return
                    end
                else
                    deduplicationKeyExists = not rcall('SET', deduplicationKey, jobId, 'PX', ttl, 'NX')
                end
            else
                deduplicationKeyExists = not rcall('SET', deduplicationKey, jobId, 'NX')
            end

            if deduplicationKeyExists then
                local currentDebounceJobId = rcall('GET', deduplicationKey)
                rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "debounced", "jobId",
                    currentDebounceJobId, "debounceId", deduplicationId)
                rcall("XADD", eventsKey, "MAXLEN", "~", maxEvents, "*", "event", "deduplicated", "jobId",
                    currentDebounceJobId, "deduplicationId", deduplicationId, "deduplicatedJobId", jobId)
                return currentDebounceJobId
            end
        end
    end
end
