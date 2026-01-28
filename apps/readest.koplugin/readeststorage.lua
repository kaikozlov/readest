local UIManager = require("ui/uimanager")
local logger = require("logger")
local socketutil = require("socketutil")

local STORAGE_TIMEOUTS = { 10, 30 }

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
    self.client = Spore.new_from_spec(self.service_spec)

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
        if not UIManager.looper then return end
        req:finalize()
        local result
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
            logger.dbg("ReadestStorageClient:requestUpload failure:", res)
            callback(false, res)
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
            logger.dbg("ReadestStorageClient:requestDownload failure:", res)
            callback(false, res)
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
            logger.dbg("ReadestStorageClient:listFiles failure:", res)
            callback(false, res)
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
            logger.dbg("ReadestStorageClient:deleteFile failure:", res)
            callback(false, res)
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
            logger.dbg("ReadestStorageClient:getStats failure:", res)
            callback(false, res)
        end
    end)
    self.client:enable("AsyncHTTP", {thread = co})
    coroutine.resume(co)
    if UIManager.looper then UIManager:setInputTimeout() end
    socketutil:reset_timeout()
end

function ReadestStorageClient:uploadFileToUrl(uploadUrl, filePath, callback)
    -- Use socket.http for direct upload to storage
    local http = require("socket.http")
    local ltn12 = require("ltn12")
    local mime = require("mime")

    local file = io.open(filePath, "rb")
    if not file then
        callback(false, {error = "Failed to open file"})
        return
    end

    local file_content = file:read("*all")
    file:close()

    local body, err = mime.encode(file_content, "binary")
    if not body then
        callback(false, {error = err})
        return
    end

    local result, code = http.request{
        url = uploadUrl,
        method = "PUT",
        headers = {
            ["Content-Type"] = "application/octet-stream",
            ["Content-Length"] = #file_content,
        },
        source = ltn12.source.string(file_content),
        sink = ltn12.sink.table({})
    }

    callback(code == 200, {status = code})
end

function ReadestStorageClient:downloadFileFromUrl(downloadUrl, filePath, callback)
    local http = require("socket.http")
    local ltn12 = require("ltn12")

    local file = io.open(filePath, "wb")
    if not file then
        callback(false, {error = "Failed to create file"})
        return
    end

    local result, code = http.request{
        url = downloadUrl,
        method = "GET",
        sink = ltn12.sink.file(file)
    }

    file:close()

    if code == 200 then
        callback(true, {path = filePath})
    else
        os.remove(filePath) -- Clean up partial download
        callback(false, {error = "Download failed", status = code})
    end
end

return ReadestStorageClient
