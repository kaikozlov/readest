local UIManager = require("ui/uimanager")
local logger = require("logger")
local socketutil = require("socketutil")

-- Storage operation timeouts (connect, total)
local STORAGE_TIMEOUTS = { 15, 60 }

local ReadestStorageClient = {
    service_spec = nil,
    access_token = nil,
    base_url = nil,
}

function ReadestStorageClient:new(o)
    if o == nil then o = {} end
    setmetatable(o, self)
    self.__index = self
    if o.init then o:init() end
    return o
end

function ReadestStorageClient:init()
    local Spore = require("Spore")
    local dkjson = require("dkjson")
    logger.warn("ReadestStorageClient: Loading spec from:", self.service_spec)

    -- Read the JSON file content
    local file = io.open(self.service_spec, "r")
    if not file then
        logger.err("ReadestStorageClient: FAILED to open JSON file!")
        return
    end
    local content = file:read("*all")
    file:close()
    logger.warn("ReadestStorageClient: JSON file content length:", #content)

    -- Parse with dkjson to verify
    local spec, pos, err = dkjson.decode(content)
    if not spec then
        logger.err("ReadestStorageClient: JSON parse error:", err)
        return
    end
    logger.warn("ReadestStorageClient: spec.base_url from dkjson:", spec.base_url)
    logger.warn("ReadestStorageClient: spec.methods:", spec.methods and "present" or "nil")

    -- Use new_from_string
    self.client = Spore.new_from_string(content)
    logger.warn("ReadestStorageClient: Client created")

    -- Check what the upload method's internal state looks like
    -- The method is a function, but we can call it and check the error to trace the issue
    -- Or we can intercept at the middleware level

    -- Headers middleware
    package.loaded["Spore.Middleware.StorageHeaders"] = {}
    require("Spore.Middleware.StorageHeaders").call = function(args, req)
        req.headers["content-type"] = "application/json"
        req.headers["accept"] = "application/json"
    end

    -- Auth middleware
    package.loaded["Spore.Middleware.StorageAuth"] = {}
    require("Spore.Middleware.StorageAuth").call = function(args, req)
        if self.access_token then
            req.headers["authorization"] = "Bearer " .. self.access_token
        else
            logger.err("ReadestStorageClient: access_token is not set")
            return false
        end
    end

    -- Async HTTP
    package.loaded["Spore.Middleware.AsyncHTTP"] = {}
    require("Spore.Middleware.AsyncHTTP").call = function(args, req)
        req:finalize()
        logger.warn("ReadestStorageClient: AsyncHTTP req.url =", req.url)
        logger.warn("ReadestStorageClient: AsyncHTTP req.method =", req.method)
        logger.warn("ReadestStorageClient: UIManager.looper =", UIManager.looper and "exists" or "nil")

        local result
        if UIManager.looper then
            -- Async request using Turbo
            require("httpclient"):new():request({
                url = req.url,
                method = req.method,
                body = req.env.spore.payload,
                on_headers = function(headers)
                    for header, value in pairs(req.headers) do
                        if type(header) == "string" then
                            headers:add(header, value)
                        end
                    end
                end
            }, function(res)
                result = res
                result.status = res.code
                coroutine.resume(args.thread)
            end)
            return coroutine.create(function() coroutine.yield(result) end)
        else
            -- Sync request using ssl.https
            local https = require("ssl.https")
            local ltn12 = require("ltn12")
            local response_body = {}

            local request_body = req.env.spore.payload or ""
            logger.warn("ReadestStorageClient: sync request_body =", request_body)
            logger.warn("ReadestStorageClient: sync headers =", req.headers)

            local ok, code, response_headers = https.request{
                url = req.url,
                method = req.method,
                headers = req.headers,
                source = ltn12.source.string(request_body),
                sink = ltn12.sink.table(response_body),
            }
            local body_str = table.concat(response_body)
            logger.warn("ReadestStorageClient: sync request result:", ok, code)
            logger.warn("ReadestStorageClient: sync response body:", body_str:sub(1, 500))

            if ok then
                return {
                    status = code,
                    headers = response_headers,
                    body = body_str,
                }
            else
                logger.warn("ReadestStorageClient: sync request failed:", code)
                return { status = 599, body = tostring(code) }
            end
        end
    end
end

function ReadestStorageClient:requestUpload(params, callback)
    self.client:reset_middlewares()
    self.client:enable("Format.JSON")
    self.client:enable("StorageHeaders", {})
    self.client:enable("StorageAuth", {})

    socketutil:set_timeout(STORAGE_TIMEOUTS[1], STORAGE_TIMEOUTS[2])
    local co = coroutine.create(function()
        local ok, res = pcall(function()
            return self.client:upload({
                fileName = params.fileName,
                fileSize = params.fileSize,
                bookHash = params.bookHash,
                temp = params.temp or false,
            })
        end)
        if ok then
            callback(res.status == 200, res.body)
        else
            logger.warn("ReadestStorageClient:requestUpload failure:", res)
            -- Pass the error as a table with error field
            if type(res) == "table" then
                callback(false, res)
            else
                callback(false, {error = tostring(res)})
            end
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:requestDownload(fileKey, callback)
    self.client:reset_middlewares()
    self.client:enable("Format.JSON")
    self.client:enable("StorageHeaders", {})
    self.client:enable("StorageAuth", {})

    socketutil:set_timeout(STORAGE_TIMEOUTS[1], STORAGE_TIMEOUTS[2])
    local co = coroutine.create(function()
        local ok, res = pcall(function()
            return self.client:download({ fileKey = fileKey })
        end)
        if ok then
            callback(res.status == 200, res.body)
        else
            logger.warn("ReadestStorageClient:requestDownload failure:", res)
            -- Pass the error as a table with error field
            if type(res) == "table" then
                callback(false, res)
            else
                callback(false, {error = tostring(res)})
            end
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:listFiles(params, callback)
    self.client:reset_middlewares()
    self.client:enable("Format.JSON")
    self.client:enable("StorageHeaders", {})
    self.client:enable("StorageAuth", {})

    socketutil:set_timeout(STORAGE_TIMEOUTS[1], STORAGE_TIMEOUTS[2])
    local co = coroutine.create(function()
        local ok, res = pcall(function()
            return self.client:list({
                page = params.page or 1,
                limit = params.limit or 50,
                orderBy = params.orderBy or "created_at",
                order = params.order or "desc",
            })
        end)
        if ok then
            callback(res.status == 200, res.body)
        else
            logger.warn("ReadestStorageClient:listFiles failure:", res)
            -- Pass the error as a table with error field
            if type(res) == "table" then
                callback(false, res)
            else
                callback(false, {error = tostring(res)})
            end
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:deleteFile(fileKey, callback)
    self.client:reset_middlewares()
    self.client:enable("Format.JSON")
    self.client:enable("StorageHeaders", {})
    self.client:enable("StorageAuth", {})

    socketutil:set_timeout(STORAGE_TIMEOUTS[1], STORAGE_TIMEOUTS[2])
    local co = coroutine.create(function()
        local ok, res = pcall(function()
            return self.client:delete({ fileKey = fileKey })
        end)
        if ok then
            callback(res.status == 200, res.body)
        else
            logger.warn("ReadestStorageClient:deleteFile failure:", res)
            -- Pass the error as a table with error field
            if type(res) == "table" then
                callback(false, res)
            else
                callback(false, {error = tostring(res)})
            end
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:getStats(callback)
    self.client:reset_middlewares()
    self.client:enable("Format.JSON")
    self.client:enable("StorageHeaders", {})
    self.client:enable("StorageAuth", {})

    socketutil:set_timeout(STORAGE_TIMEOUTS[1], STORAGE_TIMEOUTS[2])
    local co = coroutine.create(function()
        local ok, res = pcall(function()
            return self.client:stats({})
        end)
        if ok then
            callback(res.status == 200, res.body)
        else
            logger.warn("ReadestStorageClient:getStats failure:", res)
            -- Pass the error as a table with error field
            if type(res) == "table" then
                callback(false, res)
            else
                callback(false, {error = tostring(res)})
            end
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:uploadFileToUrl(uploadUrl, filePath, callback)
    -- Use ssl.https for direct upload to storage (URLs are HTTPS)
    local https = require("ssl.https")
    local ltn12 = require("ltn12")

    local file = io.open(filePath, "rb")
    if not file then
        callback(false, {error = "Failed to open file"})
        return
    end

    local file_content = file:read("*all")
    file:close()

    logger.warn("ReadestStorageClient:uploadFileToUrl url=", uploadUrl)
    logger.warn("ReadestStorageClient:uploadFileToUrl size=", #file_content)

    local response_body = {}
    local ok, code, headers = https.request{
        url = uploadUrl,
        method = "PUT",
        headers = {
            ["Content-Type"] = "application/octet-stream",
            ["Content-Length"] = tostring(#file_content),
        },
        source = ltn12.source.string(file_content),
        sink = ltn12.sink.table(response_body),
    }

    logger.warn("ReadestStorageClient:uploadFileToUrl result:", ok, code)

    if not ok then
        callback(false, {error = "HTTPS request failed: " .. tostring(code)})
        return
    end

    callback(code == 200, {status = code})
end

function ReadestStorageClient:downloadFileFromUrl(downloadUrl, filePath, callback)
    -- Use ssl.https for downloads (URLs are HTTPS)
    local https = require("ssl.https")
    local ltn12 = require("ltn12")

    local file = io.open(filePath, "wb")
    if not file then
        callback(false, {error = "Failed to create file"})
        return
    end

    logger.warn("ReadestStorageClient:downloadFileFromUrl url=", downloadUrl)

    local ok, code, headers = https.request{
        url = downloadUrl,
        method = "GET",
        sink = ltn12.sink.file(file)
    }

    file:close()

    logger.warn("ReadestStorageClient:downloadFileFromUrl result:", ok, code)

    if not ok then
        os.remove(filePath) -- Clean up partial download
        callback(false, {error = "HTTPS request failed: " .. tostring(code)})
        return
    end

    if code == 200 then
        callback(true, {path = filePath})
    else
        os.remove(filePath) -- Clean up partial download
        callback(false, {error = "Download failed", status = code})
    end
end

return ReadestStorageClient
