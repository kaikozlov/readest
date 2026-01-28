local Device = require("device")
local Event = require("ui/event")
local Dispatcher = require("dispatcher")
local InfoMessage = require("ui/widget/infomessage")
local WidgetContainer = require("ui/widget/container/widgetcontainer")
local MultiInputDialog = require("ui/widget/multiinputdialog")
local NetworkMgr = require("ui/network/manager")
local UIManager = require("ui/uimanager")
local logger = require("logger")
local time = require("ui/time")
local util = require("util")
local sha2 = require("ffi/sha2")
local T = require("ffi/util").template
local _ = require("gettext")

local ReadestSync = WidgetContainer:new{
    name = "readest",
    title = _("Readest Sync"),

    settings = nil,
}

local API_CALL_DEBOUNCE_DELAY = 30
local SUPABAE_ANON_KEY_BASE64 = "ZXlKaGJHY2lPaUpJVXpJMU5pSXNJblI1Y0NJNklrcFhWQ0o5LmV5SnBjM01pT2lKemRYQmhZbUZ6WlNJc0luSmxaaUk2SW5aaWMzbDRablZ6YW1weFpIaHJhbkZzZVhOaklpd2ljbTlzWlNJNkltRnViMjRpTENKcFlYUWlPakUzTXpReE1qTTJOekVzSW1WNGNDSTZNakEwT1RZNU9UWTNNWDAuM1U1VXFhb3VfMVNnclZlMWVvOXJBcGMwdUtqcWhwUWRVWGh2d1VIbVVmZw=="

ReadestSync.default_settings = {
    supabase_url = "https://readest.supabase.co",
    supabase_anon_key = sha2.base64_to_bin(SUPABAE_ANON_KEY_BASE64),
    auto_sync = false,
    user_email = nil,
    user_name = nil,
    user_id = nil,
    access_token = nil,
    refresh_token = nil,
    expires_at = nil,
    expires_in = nil,
    last_sync_at = nil,
    sync_queue = {},
    storage_usage = nil,
    storage_quota = nil,
}

function ReadestSync:init()
    self.last_sync_timestamp = 0
    self.settings = G_reader_settings:readSetting("readest_sync", self.default_settings)

    -- Ensure sync_queue is always initialized (for existing users)
    if self.settings.sync_queue == nil then
        self.settings.sync_queue = {}
    end
    -- Ensure storage_usage and storage_quota are initialized
    if self.settings.storage_usage == nil then
        self.settings.storage_usage = nil
    end
    if self.settings.storage_quota == nil then
        self.settings.storage_quota = nil
    end

    self.ui.menu:registerToMainMenu(self)
end

function ReadestSync:onDispatcherRegisterActions()
    Dispatcher:registerAction("readest_sync_set_autosync",
        { category="string", event="ReadestSyncToggleAutoSync", title=_("Set auto progress sync"), reader=true,
        args={true, false}, toggle={_("on"), _("off")},})
    Dispatcher:registerAction("readest_sync_toggle_autosync", { category="none", event="ReadestSyncToggleAutoSync", title=_("Toggle auto readest sync"), reader=true,})
    Dispatcher:registerAction("readest_sync_push_progress", { category="none", event="ReadestSyncPushProgress", title=_("Push readest progress from this device"), reader=true,})
    Dispatcher:registerAction("readest_sync_pull_progress", { category="none", event="ReadestSyncPullProgress", title=_("Pull readest progress from other devices"), reader=true, separator=true,})
end

function ReadestSync:onReaderReady()
    if self.settings.auto_sync and self.settings.access_token then
        UIManager:nextTick(function()
            self:pullBookConfig(false)
        end)
    end
    self:onDispatcherRegisterActions()
end

function ReadestSync:addToMainMenu(menu_items)
    menu_items.readest_sync = {
        sorting_hint = "tools",
        text_func = function()
            local status = _("Readest Sync")
            if self:needsLogin() then
                return status .. " (" .. _("Not logged in") .. ")"
            elseif #self.settings.sync_queue > 0 then
                return status .. " (" .. T(_("%1 pending"), #self.settings.sync_queue) .. ")"
            elseif self.settings.storage_usage and self.settings.storage_quota then
                local usage_pct = math.floor((self.settings.storage_usage / self.settings.storage_quota) * 100)
                return status .. " (" .. T(_("%1%% used"), usage_pct) .. ")"
            elseif self.settings.last_sync_at then
                local time_str = os.date("%H:%M", self.settings.last_sync_at)
                return status .. " (" .. T(_("Last: %1"), time_str) .. ")"
            else
                return status
            end
        end,
        sub_item_table = {
            {
                text_func = function()
                    return self:needsLogin() and _("Log in Readest Account")
                        or _("Log out as ") .. (self.settings.user_name or "")
                end,
                callback_func = function()
                    if self:needsLogin() then
                        return function(menu)
                            self:login(menu)
                        end
                    else
                        return function(menu)
                            self:logout(menu)
                        end
                    end
                end,
                separator = true,
            },
            {
                text = _("Auto sync book configs"),
                checked_func = function() return self.settings.auto_sync end,
                callback = function()
                    self:onReadestSyncToggleAutoSync()
                end,
                separator = true,
            },
            {
                text = _("Push book config now"),
                enabled_func = function()
                    return self.settings.access_token ~= nil and self.ui.document ~= nil
                end,
                callback = function()
                    self:pushBookConfig(true)
                end,
            },
            {
                text = _("Pull book config now"),
                enabled_func = function()
                    return self.settings.access_token ~= nil and self.ui.document ~= nil
                end,
                callback = function()
                    self:pullBookConfig(true)
                end,
            },
            {
                text = _("Sync all books in library"),
                enabled_func = function() return self.settings.access_token ~= nil end,
                callback = function() self:syncAllBooks(true) end,
                separator = true,
            },
            {
                text = _("Upload books to storage"),
                enabled_func = function() return self.settings.access_token ~= nil end,
                callback = function() self:showUploadBookList() end,
                separator = true,
            },
            {
                text = _("Download books from storage"),
                enabled_func = function() return self.settings.access_token ~= nil end,
                callback = function() self:showDownloadBookList() end,
            },
            {
                text = _("Storage statistics"),
                enabled_func = function() return self.settings.access_token ~= nil end,
                callback = function() self:showStorageStats() end,
            },
            {
                text = _("Delete books from storage"),
                enabled_func = function() return self.settings.access_token ~= nil end,
                callback = function() self:showDeleteBookList() end,
            },
            {
                text = _("View sync queue"),
                enabled_func = function() return #self.settings.sync_queue > 0 end,
                callback = function()
                    local queue = self.settings.sync_queue
                    if #queue == 0 then
                        UIManager:show(InfoMessage:new{
                            text = _("Sync queue is empty"),
                            timeout = 2,
                        })
                        return
                    end

                    local text = T(_("%1 items pending sync:"), #queue) .. "\n\n"
                    for i, item in ipairs(queue) do
                        local time_str = os.date("%H:%M:%S", item.timestamp)
                        text = text .. string.format("%d. [%s] %s (retries: %d)\n",
                            i, time_str, item.type, item.retries)
                    end

                    UIManager:show(InfoMessage:new{
                        text = text,
                        timeout = 5,
                    })
                end,
            },
        }
    }
end

function ReadestSync:needsLogin()
    return not self.settings.access_token or not self.settings.expires_at
        or self.settings.expires_at < os.time() + 60
end

function ReadestSync:tryRefreshToken()
    if self.settings.refresh_token and self.settings.expires_at
        and self.settings.expires_at < os.time() + self.settings.expires_in / 2 then
        local client = self:getSupabaseAuthClient()
        client:refresh_token(self.settings.refresh_token, function(success, response)
            if success then
                self.settings.access_token = response.access_token
                self.settings.refresh_token = response.refresh_token
                self.settings.expires_at = response.expires_at
                self.settings.expires_in = response.expires_in
                G_reader_settings:saveSetting("readest_sync", self.settings)
            else
                logger.err("ReadestSync: Token refresh failed:", response or "Unknown error")
            end
        end)
    end
end

function ReadestSync:getSupabaseAuthClient()
    if not self.settings.supabase_url or not self.settings.supabase_anon_key then
        return nil
    end

    local SupabaseAuthClient = require("supabaseauth")
    return SupabaseAuthClient:new{
        service_spec = self.path .. "/supabase-auth-api.json",
        custom_url = self.settings.supabase_url .. "/auth/v1/",
        api_key = self.settings.supabase_anon_key,
    }
end

function ReadestSync:getReadestSyncClient()
    if not self.settings.access_token or not self.settings.expires_at or self.settings.expires_at < os.time() then
        return nil
    end

    local ReadestSyncClient = require("readestsync")
    return ReadestSyncClient:new{
        service_spec = self.path .. "/readest-sync-api.json",
        access_token = self.settings.access_token,
    }
end

function ReadestSync:getReadestStorageClient()
    if not self.settings.access_token or not self.settings.expires_at or self.settings.expires_at < os.time() then
        return nil
    end

    local ReadestStorageClient = require("readeststorage")
    return ReadestStorageClient:new{
        service_spec = self.path .. "/readest-storage-api.json",
        access_token = self.settings.access_token,
    }
end

function ReadestSync:getBookMetadataFromFile(file_path, doc_settings)
    local doc_props = doc_settings:readSetting("doc_props")
    if not doc_props then return nil end

    local book_hash = doc_settings:readSetting("partial_md5_checksum")
    if not book_hash or book_hash == "" then return nil end

    -- Get or generate metadata hash
    local doc_readest_sync = doc_settings:readSetting("readest_sync") or {}
    local meta_hash = doc_readest_sync.meta_hash_v1
    if not meta_hash then
        local title = doc_props.title or ""
        if title == "" then
            local dir, filename = util.splitFilePathName(file_path)
            local basename, _ = util.splitFileNameSuffix(filename)
            title = basename or ""
        end

        local authors = doc_props.authors or ""
        if authors:find("\n") then
            authors = util.splitToArray(authors, "\n")
            for i, author in ipairs(authors) do
                authors[i] = normalizeAuthor(author)
            end
            authors = table.concat(authors, ",")
        else
            authors = normalizeAuthor(authors)
        end

        local identifiers = doc_props.identifiers or ""
        if identifiers:find("\n") then
            local list = util.splitToArray(identifiers, "\n")
            local normalized = {}
            local priorities = { "uuid", "calibre", "isbn" }
            local preferred = nil
            for _, id in ipairs(list) do
                table.insert(normalized, normalizeIdentifier(id))
                local candidate = id:lower()
                for _, p in ipairs(priorities) do
                    if candidate:find(p, 1, true) then
                        preferred = normalizeIdentifier(id)
                        break
                    end
                end
            end
            identifiers = preferred or table.concat(normalized, ",")
        else
            identifiers = normalizeIdentifier(identifiers)
        end

        local doc_meta = title .. "|" .. authors .. "|" .. identifiers
        meta_hash = sha2.md5(doc_meta)
    end

    local author_str = doc_props.authors or ""
    local author_list = {}
    if author_str:find("\n") then
        author_list = util.splitToArray(author_str, "\n")
    else
        table.insert(author_list, author_str)
    end

    local summary = doc_settings:readSetting("summary") or {}
    local status = summary.status or "reading"
    if status == "complete" then status = "finished"
    elseif status == "abandoned" then status = "abandoned"
    else status = "reading" end

    return {
        userId = self.settings.user_id,
        hash = book_hash,
        metaHash = meta_hash,
        format = doc_props.document_format or "unknown",
        title = doc_props.title or "",
        author = author_list[1] or "",
        groupId = nil,
        groupName = nil,
        tags = {},
        progress = nil,
        readingStatus = status,
        metadata = nil,
        createdAt = nil,
        updatedAt = os.time() * 1000,
        deletedAt = nil
    }
end

function ReadestSync:getConfigFromDocSettings(doc_settings)
    local book_hash = doc_settings:readSetting("partial_md5_checksum")
    if not book_hash or book_hash == "" then return nil end

    local doc_readest_sync = doc_settings:readSetting("readest_sync") or {}
    local meta_hash = doc_readest_sync.meta_hash_v1
    if not meta_hash or meta_hash == "" then return nil end

    local summary = doc_settings:readSetting("summary") or {}
    local config = {
        bookHash = book_hash,
        metaHash = meta_hash,
        progress = "",
        xpointer = "",
        updatedAt = os.time() * 1000,
    }

    if summary.last_page then
        local total_pages = summary.total_pages or 0
        config.progress = string.format("[%d,%d]", summary.last_page, total_pages)
    end

    local xpointer = doc_settings:readSetting("last_xpointer")
    if xpointer then config.xpointer = xpointer end

    return config
end

function ReadestSync:getAnnotationsFromDocSettings(doc_settings, book_hash, meta_hash)
    local annotations = doc_settings:readSetting("annotations") or {}
    local notes = {}

    for _, annotation in ipairs(annotations) do
        if annotation.drawer then
            local note_type = annotation.note and "annotation" or "highlight"
            local color = annotation.color

            if type(color) == "number" then
                color = string.format("#%06x", color * 0xFFFFFF)
            elseif type(color) == "table" then
                color = string.format("#%02x%02x%02x",
                    math.floor((color[1] or 0) * 255),
                    math.floor((color[2] or 0) * 255),
                    math.floor((color[3] or 0) * 255))
            end

            local updated_at = annotation.datetime_updated or annotation.datetime
            if type(updated_at) == "number" then
                updated_at = updated_at * 1000
            end

            table.insert(notes, {
                userId = self.settings.user_id,
                bookHash = book_hash,
                metaHash = meta_hash,
                id = self:generateNoteId(annotation),
                type = note_type,
                cfi = annotation.page,
                text = annotation.text or "",
                style = annotation.drawer or "highlight",
                color = color or "#FFFF00",
                note = annotation.note or "",
                updatedAt = updated_at,
                deletedAt = nil
            })
        end
    end

    return notes
end

function ReadestSync:syncAllBooks(interactive)
    if not self.settings.access_token then
        if interactive then
            UIManager:show(InfoMessage:new{ text = _("Please login first"), timeout = 2 })
        end
        return
    end

    if interactive and NetworkMgr:willRerunWhenOnline(function() self:syncAllBooks(interactive) end) then
        return
    end

    local ReadHistory = require("readhistory")
    local DocSettings = require("docsettings")

    if interactive then
        UIManager:show(InfoMessage:new{ text = _("Scanning library..."), timeout = 1 })
    end

    local books = {}
    local all_notes = {}
    local all_configs = {}
    local processed = 0

    for _, v in ipairs(ReadHistory.hist) do
        if v.dim then goto continue end

        local ok, doc_settings = pcall(function() return DocSettings:open(v.file) end)
        if not ok or not doc_settings then goto continue end

        local metadata = self:getBookMetadataFromFile(v.file, doc_settings)
        if metadata then table.insert(books, metadata) end

        local book_hash = doc_settings:readSetting("partial_md5_checksum")
        local doc_readest_sync = doc_settings:readSetting("readest_sync") or {}
        local meta_hash = doc_readest_sync.meta_hash_v1

        if book_hash and meta_hash then
            local annotations = self:getAnnotationsFromDocSettings(doc_settings, book_hash, meta_hash)
            for _, note in ipairs(annotations) do
                table.insert(all_notes, note)
            end

            local config = self:getConfigFromDocSettings(doc_settings)
            if config then table.insert(all_configs, config) end
        end

        processed = processed + 1
        ::continue::
    end

    if interactive then
        UIManager:show(InfoMessage:new{ text = T(_("Syncing %1 books..."), processed), timeout = 1 })
    end

    local client = self:getReadestSyncClient()
    if not client then
        if interactive then
            UIManager:show(InfoMessage:new{ text = _("Failed to get sync client"), timeout = 2 })
        end
        return
    end

    self:tryRefreshToken()

    local payload = { books = books, notes = all_notes, configs = all_configs }

    client:pushChanges(payload, function(success, response)
        if interactive then
            if success then
                UIManager:show(InfoMessage:new{
                    text = T(_("Synced %1 books"), processed),
                    timeout = 3,
                })
                self.settings.last_sync_at = os.time()
                G_reader_settings:saveSetting("readest_sync", self.settings)
            else
                local error_msg = _("Sync failed")
                if response then
                    if type(response) == "table" then
                        error_msg = error_msg .. ": " .. (response.error or response.message or "Unknown error")
                    elseif type(response) == "string" then
                        error_msg = error_msg .. ": " .. response
                    end
                end
                logger.err("ReadestSync: syncAllBooks failed:", response)
                UIManager:show(InfoMessage:new{ text = error_msg, timeout = 3 })
            end
        end
    end)
end

function ReadestSync:showUploadBookList()
    if not self.settings.access_token then
        UIManager:show(InfoMessage:new{ text = _("Please login first"), timeout = 2 })
        return
    end

    if NetworkMgr:willRerunWhenOnline(function() self:showUploadBookList() end) then
        return
    end

    local ReadHistory = require("readhistory")
    local DocSettings = require("docsettings")
    local Menu = require("ui/widget/menu")

    -- Build book list
    local book_table = {}
    for _, v in ipairs(ReadHistory.hist) do
        if v.dim then goto continue end

        local ok, doc_settings = pcall(function() return DocSettings:open(v.file) end)
        if not ok or not doc_settings then goto continue end

        local doc_props = doc_settings:readSetting("doc_props")
        if not doc_props then goto continue end

        local title = doc_props.title or v.file
        local authors = doc_props.authors or _("Unknown")
        local book_hash = doc_settings:readSetting("partial_md5_checksum")

        table.insert(book_table, {
            text = title .. " - " .. authors,
            filepath = v.file,
            book_hash = book_hash,
            doc_settings = doc_settings,
            doc_props = doc_props,
        })

        ::continue::
    end

    if #book_table == 0 then
        UIManager:show(InfoMessage:new{ text = _("No books found in library"), timeout = 2 })
        return
    end

    -- Show menu
    local menu = Menu:new{
        title = _("Select books to upload"),
        item_table = book_table,
        is_borderless = true,
        is_popout = false,
        select_mode = "multi",
        callback = function()
            local selected = menu:getMultiSelection()
            if #selected > 0 then
                UIManager:close(menu)
                self:uploadSelectedBooks(selected)
            end
        end,
        close_callback = function()
            UIManager:close(menu)
        end
    }
    UIManager:show(menu)
end

function ReadestSync:uploadSelectedBooks(selections)
    UIManager:show(InfoMessage:new{
        text = T(_("Preparing to upload %1 books..."), #selections),
        timeout = 1,
    })

    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    self:tryRefreshToken()

    -- Upload books sequentially
    local index = 0
    local success_count = 0
    local fail_count = 0

    local function uploadNext()
        index = index + 1
        if index > #selections then
            UIManager:show(InfoMessage:new{
                text = T(_("Upload complete: %1 succeeded, %2 failed"), success_count, fail_count),
                timeout = 3,
            })
            return
        end

        local selection = selections[index]
        local doc_settings = selection.doc_settings
        local book_hash = doc_settings:readSetting("partial_md5_checksum")

        if not book_hash then
            logger.warn("ReadestSync: Book has no hash, skipping:", selection.filepath)
            fail_count = fail_count + 1
            UIManager:scheduleIn(0.1, uploadNext)
            return
        end

        UIManager:show(InfoMessage:new{
            text = T(_("Uploading %1 of %2..."), index, #selections),
            timeout = 1,
        })

        -- Get file info
        local file = io.open(selection.filepath, "rb")
        if not file then
            logger.warn("ReadestSync: Failed to open file:", selection.filepath)
            fail_count = fail_count + 1
            UIManager:scheduleIn(0.1, uploadNext)
            return
        end

        local file_size = file:seek("end")
        file:close()

        local dir, filename = util.splitFilePathName(selection.filepath)

        -- Request upload URL
        client:requestUpload({
            fileName = filename,
            fileSize = file_size,
            bookHash = book_hash,
        }, function(success, response)
            if success and response and (response.uploadUrl or response.upload_url) then
                -- Upload file directly to storage
                local upload_url = response.uploadUrl or response.upload_url
                client:uploadFileToUrl(upload_url, selection.filepath, function(upload_ok, upload_res)
                    if upload_ok then
                        success_count = success_count + 1
                        logger.dbg("ReadestSync: Uploaded:", selection.filepath)
                    else
                        fail_count = fail_count + 1
                        logger.err("ReadestSync: Upload failed:", selection.filepath, upload_res)
                    end
                    UIManager:scheduleIn(0.1, uploadNext)
                end)
            else
                fail_count = fail_count + 1
                local error_info = response
                if type(response) == "table" then
                    error_info = response.error or response.message or "Unknown error"
                end
                logger.err("ReadestSync: Failed to get upload URL:", error_info)
                UIManager:scheduleIn(0.1, uploadNext)
            end
        end)
    end

    uploadNext()
end

function ReadestSync:showDownloadBookList()
    if not self.settings.access_token then
        UIManager:show(InfoMessage:new{ text = _("Please login first"), timeout = 2 })
        return
    end

    if NetworkMgr:willRerunWhenOnline(function() self:showDownloadBookList() end) then
        return
    end

    UIManager:show(InfoMessage:new{ text = _("Fetching book list..."), timeout = 1 })

    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    self:tryRefreshToken()

    client:listFiles({ limit = 100 }, function(success, response)
        if not success then
            local error_msg = _("Failed to fetch book list")
            if response then
                if type(response) == "table" then
                    error_msg = error_msg .. ": " .. (response.error or response.message or "Unknown error")
                elseif type(response) == "string" then
                    error_msg = error_msg .. ": " .. response
                end
            end
            logger.err("ReadestSync: listFiles failed:", response)
            UIManager:show(InfoMessage:new{ text = error_msg, timeout = 3 })
            return
        end

        local files = response.files or response.data or {}
        if #files == 0 then
            UIManager:show(InfoMessage:new{ text = _("No books found in storage"), timeout = 2 })
            return
        end

        self:showDownloadSelection(files)
    end)
end

function ReadestSync:showDownloadSelection(files)
    local Menu = require("ui/widget/menu")
    local local_dir = self:getDownloadDirectory()

    -- Build file list
    local file_table = {}
    for _, file in ipairs(files) do
        local fileKey = file.file_key or file.fileKey
        if not fileKey then
            logger.warn("ReadestSync: Skipping file without file_key:", file)
            goto continue
        end

        local fileName = file.file_name or file.fileName or fileKey:match("/([^/]+)$") or "unknown"
        local fileSize = file.file_size or file.fileSize or 0
        local bookHash = file.book_hash or file.bookHash

        -- Check if file exists locally
        local local_path = local_dir .. "/" .. fileName
        local exists = util.pathExists(local_path)
        local local_hash = nil

        if exists then
            -- Get local hash
            local DocSettings = require("docsettings")
            local ok, doc_settings = pcall(function() return DocSettings:open(local_path) end)
            if ok and doc_settings then
                local_hash = doc_settings:readSetting("partial_md5_checksum")
            end
        end

        local status_text = ""
        if exists then
            if local_hash == bookHash then
                status_text = " (✓ up to date)"
            else
                status_text = " (⚠ different version)"
            end
        else
            status_text = " (new)"
        end

        table.insert(file_table, {
            text = fileName .. status_text,
            file_key = fileKey,
            file_name = fileName,
            book_hash = bookHash,
            file_size = fileSize,
            local_exists = exists,
            local_hash = local_hash,
            local_path = local_path,
        })
        ::continue::
    end

    local menu = Menu:new{
        title = _("Select books to download"),
        item_table = file_table,
        is_borderless = true,
        is_popout = false,
        select_mode = "multi",
        callback = function()
            local selected = menu:getMultiSelection()
            if #selected > 0 then
                UIManager:close(menu)
                self:downloadSelectedBooks(selected, local_dir)
            end
        end,
        close_callback = function()
            UIManager:close(menu)
        end
    }
    UIManager:show(menu)
end

function ReadestSync:getDownloadDirectory()
    -- Use KOReader's download directory or default to home
    local download_dir = G_reader_settings:readSetting("download_dir") or
                         os.getenv("HOME") .. "/Downloads"

    -- Ensure directory exists
    if not util.pathExists(download_dir) then
        util.makePath(download_dir)
    end

    return download_dir
end

function ReadestSync:downloadSelectedBooks(selections, download_dir)
    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    -- Check for conflicts
    local conflicts = {}
    local no_conflicts = {}

    for _, selection in ipairs(selections) do
        if selection.local_exists and selection.local_hash ~= selection.book_hash then
            table.insert(conflicts, selection)
        else
            table.insert(no_conflicts, selection)
        end
    end

    -- Handle conflicts
    if #conflicts > 0 then
        self:handleConflicts(conflicts, no_conflicts, download_dir, client)
    else
        self:downloadBooks(no_conflicts, download_dir, client)
    end
end

function ReadestSync:handleConflicts(conflicts, no_conflicts, download_dir, client)
    local ButtonDialog = require("ui/widget/buttondialog")
    local ConfirmWidget = require("ui/widget/confirmbox")

    -- For each conflict, ask user what to do
    local conflict_index = 0

    local function handleNextConflict()
        conflict_index = conflict_index + 1
        if conflict_index > #conflicts then
            -- All conflicts handled, proceed with downloads
            self:downloadBooks(no_conflicts, download_dir, client)
            return
        end

        local conflict = conflicts[conflict_index]
        local size_mb = string.format("%.2f MB", conflict.file_size / (1024 * 1024))

        UIManager:show(ConfirmWidget:new{
            text = T(_("%1\nSize: %2\n\nA different version exists locally.\nLocal hash: %3\nRemote hash: %4\n\nOverwrite local file?"),
                conflict.file_name, size_mb,
                conflict.local_hash or "unknown",
                conflict.book_hash or "unknown"),
            ok_text = _("Overwrite"),
            ok_callback = function()
                table.insert(no_conflicts, conflict)
                UIManager:scheduleIn(0.1, handleNextConflict)
            end,
            cancel_text = _("Skip"),
            cancel_callback = function()
                UIManager:scheduleIn(0.1, handleNextConflict)
            end
        })
    end

    handleNextConflict()
end

function ReadestSync:downloadBooks(selections, download_dir, client)
    UIManager:show(InfoMessage:new{
        text = T(_("Downloading %1 books..."), #selections),
        timeout = 1,
    })

    local index = 0
    local success_count = 0
    local fail_count = 0
    local skipped_count = 0

    local function downloadNext()
        index = index + 1
        if index > #selections then
            UIManager:show(InfoMessage:new{
                text = T(_("Download complete: %1 succeeded, %2 failed, %3 skipped"),
                    success_count, fail_count, skipped_count),
                timeout = 3,
            })
            return
        end

        local selection = selections[index]

        -- Skip if up to date
        if selection.local_exists and selection.local_hash == selection.book_hash then
            skipped_count = skipped_count + 1
            UIManager:scheduleIn(0.1, downloadNext)
            return
        end

        UIManager:show(InfoMessage:new{
            text = T(_("Downloading %1 of %2..."), index, #selections),
            timeout = 1,
        })

        -- Request download URL
        client:requestDownload(selection.file_key, function(success, response)
            if success and response and (response.downloadUrl or response.download_url) then
                local download_url = response.downloadUrl or response.download_url
                local dest_path = download_dir .. "/" .. selection.file_name

                -- Download file
                client:downloadFileFromUrl(download_url, dest_path, function(download_ok, download_res)
                    if download_ok then
                        success_count = success_count + 1
                        logger.dbg("ReadestSync: Downloaded:", selection.file_name)

                        -- Add to KOReader library
                        local FileManager = require("apps/filemanager/filemanager")
                        if FileManager.instance then
                            FileManager.instance:onRefresh()
                        end
                    else
                        fail_count = fail_count + 1
                        logger.err("ReadestSync: Download failed:", selection.file_name, download_res)
                    end
                    UIManager:scheduleIn(0.1, downloadNext)
                end)
            else
                fail_count = fail_count + 1
                logger.err("ReadestSync: Failed to get download URL:", response)
                UIManager:scheduleIn(0.1, downloadNext)
            end
        end)
    end

    downloadNext()
end

function ReadestSync:extractCoverImage(doc_settings)
    -- Get cover from KOReader's cache
    local doc_props = doc_settings:readSetting("doc_props")
    if not doc_props then return nil end

    local cover_file = doc_settings:readSetting("cover_file")
    if not cover_file then
        -- Try to extract cover from document
        if self.ui and self.ui.document and self.ui.document.getCoverPageImage then
            local ok, image = pcall(function()
                return self.ui.document:getCoverPageImage()
            end)
            if ok and image then
                -- Save to temp file
                local temp_dir = os.getenv("TMPDIR") or "/tmp"
                local temp_cover = temp_dir .. "/readest_cover_" .. os.time() .. ".png"
                local gd = require("gd")
                local gd_img = gd.fromImage(image)
                if gd_img then
                    gd_img:png(temp_cover)
                    return temp_cover
                end
            end
        end
        return nil
    end

    return cover_file
end

function ReadestSync:uploadCoverForBook(book_hash, doc_settings)
    local client = self:getReadestStorageClient()
    if not client then return end

    local cover_path = self:extractCoverImage(doc_settings)
    if not cover_path or not util.pathExists(cover_path) then
        logger.dbg("ReadestSync: No cover found for book:", book_hash)
        return
    end

    local file = io.open(cover_path, "rb")
    if not file then return end

    local file_size = file:seek("end")
    file:close()

    local filename = "cover_" .. book_hash .. ".png"

    client:requestUpload({
        fileName = filename,
        fileSize = file_size,
        bookHash = book_hash,
        temp = false,
    }, function(success, response)
        if success and response.uploadUrl then
            client:uploadFileToUrl(response.uploadUrl, cover_path, function(upload_ok)
                if upload_ok then
                    logger.dbg("ReadestSync: Uploaded cover for:", book_hash)
                else
                    logger.err("ReadestSync: Failed to upload cover:", book_hash)
                end
                -- Clean up temp file
                if cover_path:find("/tmp/") then
                    os.remove(cover_path)
                end
            end)
        end
    end)
end

function ReadestSync:showStorageStats()
    if not self.settings.access_token then
        UIManager:show(InfoMessage:new{ text = _("Please login first"), timeout = 2 })
        return
    end

    if NetworkMgr:willRerunWhenOnline(function() self:showStorageStats() end) then
        return
    end

    UIManager:show(InfoMessage:new{ text = _("Fetching storage stats..."), timeout = 1 })

    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    self:tryRefreshToken()

    client:getStats(function(success, response)
        if not success then
            UIManager:show(InfoMessage:new{ text = _("Failed to fetch storage stats"), timeout = 2 })
            return
        end

        local totalFiles = response.totalFiles or 0
        local totalSize = response.totalSize or 0
        local usage = response.usage or 0
        local quota = response.quota or 0
        local percentage = response.usagePercentage or 0

        -- Save stats to settings
        self.settings.storage_usage = usage
        self.settings.storage_quota = quota
        G_reader_settings:saveSetting("readest_sync", self.settings)

        local size_mb = string.format("%.2f MB", totalSize / (1024 * 1024))
        local quota_mb = string.format("%.2f MB", quota / (1024 * 1024))
        local usage_mb = string.format("%.2f MB", usage / (1024 * 1024))
        local pct_str = string.format("%.1f%%", percentage * 100)

        local text = T(_("Storage Statistics\n\n") ..
                       _("Total files: %1\n") ..
                       _("Total size: %2\n") ..
                       _("Storage used: %3 of %4 (%5)"),
                       totalFiles, size_mb, usage_mb, quota_mb, pct_str)

        UIManager:show(InfoMessage:new{
            text = text,
            timeout = 5,
        })
    end)
end

function ReadestSync:showDeleteBookList()
    if not self.settings.access_token then
        UIManager:show(InfoMessage:new{ text = _("Please login first"), timeout = 2 })
        return
    end

    if NetworkMgr:willRerunWhenOnline(function() self:showDeleteBookList() end) then
        return
    end

    UIManager:show(InfoMessage:new{ text = _("Fetching book list..."), timeout = 1 })

    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    self:tryRefreshToken()

    client:listFiles({ limit = 100 }, function(success, response)
        if not success then
            local error_msg = _("Failed to fetch book list")
            if response then
                if type(response) == "table" then
                    error_msg = error_msg .. ": " .. (response.error or response.message or "Unknown error")
                elseif type(response) == "string" then
                    error_msg = error_msg .. ": " .. response
                end
            end
            logger.err("ReadestSync: listFiles failed:", response)
            UIManager:show(InfoMessage:new{ text = error_msg, timeout = 3 })
            return
        end

        local files = response.files or response.data or {}
        if #files == 0 then
            UIManager:show(InfoMessage:new{ text = _("No books found in storage"), timeout = 2 })
            return
        end

        self:showDeleteSelection(files)
    end)
end

function ReadestSync:showDeleteSelection(files)
    local Menu = require("ui/widget/menu")

    -- Build file list
    local file_table = {}
    for _, file in ipairs(files) do
        local fileKey = file.file_key or file.fileKey
        if not fileKey then
            logger.warn("ReadestSync: Skipping file without file_key:", file)
            goto continue
        end

        local fileName = file.file_name or file.fileName or fileKey:match("/([^/]+)$") or "unknown"
        local fileSize = file.file_size or file.fileSize or 0
        local size_mb = string.format("%.2f MB", fileSize / (1024 * 1024))

        table.insert(file_table, {
            text = fileName .. " (" .. size_mb .. ")",
            file_key = fileKey,
            file_name = fileName,
        })
        ::continue::
    end

    local menu = Menu:new{
        title = _("Select books to delete from storage"),
        item_table = file_table,
        is_borderless = true,
        is_popout = false,
        select_mode = "multi",
        callback = function()
            local selected = menu:getMultiSelection()
            if #selected > 0 then
                UIManager:close(menu)
                self:confirmDeleteBooks(selected)
            end
        end,
        close_callback = function()
            UIManager:close(menu)
        end
    }
    UIManager:show(menu)
end

function ReadestSync:confirmDeleteBooks(selections)
    local ConfirmWidget = require("ui/widget/confirmbox")

    UIManager:show(ConfirmWidget:new{
        text = T(_("Are you sure you want to delete %1 books from storage?\n\nThis action cannot be undone."), #selections),
        ok_text = _("Delete"),
        cancel_text = _("Cancel"),
        ok_callback = function()
            self:deleteSelectedBooks(selections)
        end
    })
end

function ReadestSync:deleteSelectedBooks(selections)
    UIManager:show(InfoMessage:new{
        text = T(_("Deleting %1 books..."), #selections),
        timeout = 1,
    })

    local client = self:getReadestStorageClient()
    if not client then
        UIManager:show(InfoMessage:new{ text = _("Failed to get storage client"), timeout = 2 })
        return
    end

    local index = 0
    local success_count = 0
    local fail_count = 0

    local function deleteNext()
        index = index + 1
        if index > #selections then
            UIManager:show(InfoMessage:new{
                text = T(_("Delete complete: %1 succeeded, %2 failed"), success_count, fail_count),
                timeout = 3,
            })
            return
        end

        local selection = selections[index]

        UIManager:show(InfoMessage:new{
            text = T(_("Deleting %1 of %2..."), index, #selections),
            timeout = 1,
        })

        client:deleteFile(selection.file_key, function(success, response)
            if success then
                success_count = success_count + 1
                logger.dbg("ReadestSync: Deleted:", selection.file_name)
            else
                fail_count = fail_count + 1
                logger.err("ReadestSync: Delete failed:", selection.file_name, response)
            end
            UIManager:scheduleIn(0.1, deleteNext)
        end)
    end

    deleteNext()
end

function ReadestSync:enqueueSync(type, data)
    table.insert(self.settings.sync_queue, {
        type = type,
        data = data,
        timestamp = os.time(),
        retries = 0
    })
    G_reader_settings:saveSetting("readest_sync", self.settings)
    logger.dbg("ReadestSync: Enqueued", type, "to sync queue")
end

function ReadestSync:processSyncQueue()
    if not NetworkMgr:isConnected() then
        return
    end

    if #self.settings.sync_queue == 0 then
        return
    end

    local client = self:getReadestSyncClient()
    if not client then
        return
    end

    logger.dbg("ReadestSync: Processing sync queue, items:", #self.settings.sync_queue)

    local queue = self.settings.sync_queue
    local to_remove = {}

    for i, item in ipairs(queue) do
        if item.retries >= 3 then
            table.insert(to_remove, i)
            logger.warn("ReadestSync: Discarding queued sync after 3 retries")
            goto continue
        end

        local success = false
        if item.type == "push" then
            client:pushChanges(item.data, function(s, r)
                success = s
                if not s then
                    item.retries = item.retries + 1
                    logger.dbg("ReadestSync: Queue push failed, retry", item.retries)
                end
            end)
        elseif item.type == "pull" then
            -- Handle queued pull
            client:pullChanges(item.data.params, function(s, r)
                success = s
                if s and item.data.onSuccess then
                    item.data.onSuccess(r)
                end
                if not s then
                    item.retries = item.retries + 1
                end
            end)
        end

        if success then
            table.insert(to_remove, i)
        end

        ::continue::
    end

    -- Remove processed items (in reverse order)
    for i = #to_remove, 1, -1 do
        table.remove(self.settings.sync_queue, to_remove[i])
    end

    G_reader_settings:saveSetting("readest_sync", self.settings)
end

function ReadestSync:onNetworkConnected()
    UIManager:nextTick(function()
        self:processSyncQueue()
    end)
end

function ReadestSync:login(menu)
    if NetworkMgr:willRerunWhenOnline(function() self:login(menu) end) then
        return
    end

    local dialog
    dialog = MultiInputDialog:new{
        title = self.title,
        fields = {
            {
                text = self.settings.user_email,
                hint = "email@example.com",
            },
            {
                hint = "password",
                text_type = "password",
            },
        },
        buttons = {
            {
                {
                    text = _("Cancel"),
                    id = "close",
                    callback = function()
                        UIManager:close(dialog)
                    end,
                },
                {
                    text = _("Login"),
                    callback = function()
                        local email, password = unpack(dialog:getFields())
                        email = util.trim(email)
                        if email == "" or password == "" then
                            UIManager:show(InfoMessage:new{
                                text = _("Please enter both email and password"),
                                timeout = 2,
                            })
                            return
                        end
                        UIManager:close(dialog)
                        self:doLogin(email, password, menu)
                    end,
                },
            },
        },
    }
    UIManager:show(dialog)
    dialog:onShowKeyboard()
end

function ReadestSync:doLogin(email, password, menu)
    local client = self:getSupabaseAuthClient()
    if not client then
        UIManager:show(InfoMessage:new{
            text = _("Please configure Supabase URL and API key first"),
            timeout = 3,
        })
        return
    end

    UIManager:show(InfoMessage:new{
        text = _("Logging in..."),
        timeout = 1,
    })

    Device:setIgnoreInput(true)
    local success, response = client:sign_in_password(email, password)
    Device:setIgnoreInput(false)

    if success then
        self.settings.user_email = email
        self.settings.user_id = response.user.id
        self.settings.user_name = response.user.user_metadata.user_name or email
        self.settings.access_token = response.access_token
        self.settings.refresh_token = response.refresh_token
        self.settings.expires_at = response.expires_at
        self.settings.expires_in = response.expires_in
        G_reader_settings:saveSetting("readest_sync", self.settings)

        if menu then
            menu:updateItems()
        end
        
        UIManager:show(InfoMessage:new{
            text = _("Successfully logged in to Readest"),
            timeout = 3,
        })
    else
        UIManager:show(InfoMessage:new{
            text = _("Login failed: ") .. (response.msg or "Unknown error"),
            timeout = 3,
        })
    end
end

function ReadestSync:logout(menu)
    if self.access_token then
        local client = self:getSupabaseAuthClient()
        if client then
            client:sign_out(self.settings.access_token, function(success, response)
                logger.dbg("ReadestSync: Sign out result:", success)
            end)
        end
    end

    self.settings.access_token = nil
    self.settings.refresh_token = nil
    self.settings.expires_at = nil
    self.settings.expires_in = nil
    G_reader_settings:saveSetting("readest_sync", self.settings)

    if menu then
        menu:updateItems()
    end
    
    UIManager:show(InfoMessage:new{
        text = _("Logged out from Readest Sync"),
        timeout = 2,
    })
end

function normalizeIdentifier(identifier)
    if identifier:match("urn:") then
        -- Slice after the last ':'
        return identifier:match("([^:]+)$")
    elseif identifier:match(":") then
        -- Slice after the first ':'
        return identifier:match("^[^:]+:(.+)$")
    end
    return identifier
end

function normalizeAuthor(author)
    -- Trim leading and trailing whitespace
    author = author:gsub("^%s*(.-)%s*$", "%1")
    return author
end

function ReadestSync:generateMetadataHash()
    local doc_props = self.ui.doc_settings:readSetting("doc_props") or {}
    local title = doc_props.title or ''
    if title == '' then
        local doc_path, filename = util.splitFilePathName(self.ui.doc_settings:readSetting("doc_path") or '')
        local basename, suffix = util.splitFileNameSuffix(filename)
        title = basename or ''
    end

    local authors = doc_props.authors or ''
    if authors:find("\n") then
        authors = util.splitToArray(authors, "\n")
        for i, author in ipairs(authors) do
            authors[i] = normalizeAuthor(author)
        end
        authors = table.concat(authors, ",")
    else
        authors = normalizeAuthor(authors)
    end

    local identifiers = doc_props.identifiers or ''
    if identifiers:find("\n") then
        local list = util.splitToArray(identifiers, "\n")
        local normalized = {}
        local priorities = { "uuid", "calibre", "isbn" }
        local preferred = nil
        for i, id in ipairs(list) do
            normalized[i] = normalizeIdentifier(id)
            local candidate = id:lower()
            for _, p in ipairs(priorities) do
                if candidate:find(p, 1, true) then
                    preferred = normalized[i]
                    break
                end
            end
        end
        if preferred then
            identifiers = preferred
        else
            identifiers = table.concat(normalized, ",")
        end
    else
        identifiers = normalizeIdentifier(identifiers)
    end
    local doc_meta = title .. "|" .. authors .. "|" .. identifiers
    local meta_hash = sha2.md5(doc_meta)
    return meta_hash
end

function ReadestSync:getMetaHash()
    local doc_readest_sync = self.ui.doc_settings:readSetting("readest_sync") or {}
    local meta_hash = doc_readest_sync.meta_hash_v1
    if not meta_hash then
        meta_hash = self:generateMetadataHash()
        doc_readest_sync.meta_hash_v1 = meta_hash
        self.ui.doc_settings:saveSetting("readest_sync", doc_readest_sync)
    end
    return meta_hash
end

function ReadestSync:getDocumentIdentifier()
    return self.ui.doc_settings:readSetting("partial_md5_checksum")
end

function ReadestSync:showSyncedMessage()
    UIManager:show(InfoMessage:new{
        text = _("Progress has been synchronized."),
        timeout = 3,
    })
end

function ReadestSync:applyBookConfig(config)
    logger.dbg("ReadestSync: Applying book config:", config)
    local xpointer = config.xpointer
    local progress = config.progress
    local has_pages = self.ui.document.info.has_pages
    -- Check if it's the bracket format: [page,total_pages]
    local progress_pattern = "^%[(%d+),(%d+)%]$"
    if has_pages and progress then
        local page, total_pages = progress:match(progress_pattern)
        local current_page = self.ui:getCurrentPage()
        local new_page = tonumber(page)
        if new_page > current_page then
            self.ui.link:addCurrentLocationToStack()
            self.ui:handleEvent(Event:new("GotoPage", new_page))
            self:showSyncedMessage()
        end
    end
    if not has_pages and xpointer then
        local last_xpointer = self.ui.rolling:getLastProgress()
        local working_xpointer = xpointer
        local cmp_result = self.document:compareXPointers(last_xpointer, working_xpointer)
        -- FIXME: Crengine is not very good at comparing XPointers, so we need to reduce the path
        while cmp_result == nil and working_xpointer do
            local last_slash_pos = working_xpointer:match("^.*()/")
            if last_slash_pos and last_slash_pos > 1 then
                working_xpointer = working_xpointer:sub(1, last_slash_pos - 1)
                cmp_result = self.document:compareXPointers(last_xpointer, working_xpointer)
            else
                break
            end
        end
        if cmp_result > 0 then
            self.ui.link:addCurrentLocationToStack()
            self.ui:handleEvent(Event:new("GotoXPointer", working_xpointer))
            self:showSyncedMessage()
        end
    end
end

function ReadestSync:generateNoteId(annotation)
    local pos0_str = annotation.pos0 and string.format("%.0f_%.0f", annotation.pos0.x or 0, annotation.pos0.y or 0) or ""
    local pos1_str = annotation.pos1 and string.format("%.0f_%.0f", annotation.pos1.x or 0, annotation.pos1.y or 0) or ""
    local data = annotation.page .. "|" .. pos0_str .. "|" .. pos1_str .. "|" .. annotation.datetime
    return sha2.md5(data)
end

function ReadestSync:getCurrentBookNotes()
    if not self.ui.annotation or #self.ui.annotation.annotations == 0 then
        return {}
    end

    local book_hash = self:getDocumentIdentifier()
    local meta_hash = self:getMetaHash()
    local notes = {}

    for _, annotation in ipairs(self.ui.annotation.annotations) do
        -- Skip bookmarks (no drawer = not a highlight)
        if not annotation.drawer then
            goto continue
        end

        local note_type = annotation.note and "annotation" or "highlight"
        local color = annotation.color

        -- Convert KOReader color to Readest format
        if type(color) == "number" then
            color = string.format("#%06x", color * 0xFFFFFF)
        elseif type(color) == "table" then
            color = string.format("#%02x%02x%02x",
                math.floor((color[1] or 0) * 255),
                math.floor((color[2] or 0) * 255),
                math.floor((color[3] or 0) * 255))
        end

        local updated_at = annotation.datetime_updated or annotation.datetime
        if type(updated_at) == "number" then
            updated_at = updated_at * 1000
        end

        table.insert(notes, {
            userId = self.settings.user_id,
            bookHash = book_hash,
            metaHash = meta_hash,
            id = self:generateNoteId(annotation),
            type = note_type,
            cfi = annotation.page,
            text = annotation.text or "",
            style = annotation.drawer or "highlight",
            color = color or "#FFFF00",
            note = annotation.note or "",
            updatedAt = updated_at,
            deletedAt = nil
        })
        ::continue::
    end

    return notes
end

function ReadestSync:applyNotesToBook(notes)
    if not notes or #notes == 0 then
        return
    end

    local current_annotations = self.ui.annotation.annotations
    local notes_to_add = {}

    for _, remote_note in ipairs(notes) do
        -- Check if note already exists
        local exists = false
        for _, local_ann in ipairs(current_annotations) do
            local local_id = self:generateNoteId(local_ann)
            if local_id == remote_note.id then
                exists = true
                break
            end
        end

        if not exists and remote_note.deletedAt == nil then
            -- Parse color back to KOReader format
            local color = remote_note.color
            if color:match("#%x%x%x%x%x%x") then
                local r = tonumber(color:sub(2, 3), 16) / 255
                local g = tonumber(color:sub(4, 5), 16) / 255
                local b = tonumber(color:sub(6, 7), 16) / 255
                color = { r, g, b }
            end

            table.insert(notes_to_add, {
                datetime = remote_note.updatedAt / 1000,
                datetime_updated = nil,
                drawer = remote_note.style,
                color = color,
                text = remote_note.text,
                note = remote_note.note,
                page = remote_note.cfi,
            })
        end
    end

    for _, annotation in ipairs(notes_to_add) do
        self.ui.annotation:addItem(annotation)
    end

    if #notes_to_add > 0 then
        self.ui.doc_settings:saveSetting("annotations", self.ui.annotation.annotations)
        UIManager:show(InfoMessage:new{
            text = T(N_("Imported 1 note", "Imported %1 notes", #notes_to_add), #notes_to_add),
            timeout = 2,
        })
    end
end

function ReadestSync:getCurrentBookConfig()
    local book_hash = self:getDocumentIdentifier()
    local meta_hash = self:getMetaHash()
    if not book_hash or not meta_hash then
        UIManager:show(InfoMessage:new{
            text = _("Cannot identify the current book"),
            timeout = 2,
        })
        return nil
    end

    local config = {
        bookHash = book_hash,
        metaHash = meta_hash,
        progress = "",
        xpointer = "",
        updatedAt = os.time() * 1000,
    }

    local current_page = self.ui:getCurrentPage()
    local page_count = self.ui.document:getPageCount()
    config.progress = {current_page, page_count}

    if not self.ui.document.info.has_pages then
        config.xpointer = self.ui.rolling:getLastProgress()
    end

    return config
end

function ReadestSync:pushBookConfig(interactive)
    if not self.settings.access_token or not self.settings.user_id then
        if interactive then
            UIManager:show(InfoMessage:new{
                text = _("Please login first"),
                timeout = 2,
            })
        end
        return
    end

    local now = os.time()
    if not interactive and now - self.last_sync_timestamp <= API_CALL_DEBOUNCE_DELAY then
        logger.dbg("ReadestSync: Debouncing push request")
        return
    end

    local config = self:getCurrentBookConfig()
    if not config then return end

    if interactive and NetworkMgr:willRerunWhenOnline(function() self:pushBookConfig(interactive) end) then
        return
    end

    local client = self:getReadestSyncClient()
    if not client then
        if interactive then
            UIManager:show(InfoMessage:new{
                text = _("Please configure Readest settings first"),
                timeout = 3,
            })
        end
        return
    end

    self:tryRefreshToken()

    if interactive then
        UIManager:show(InfoMessage:new{
            text = _("Pushing book config..."),
            timeout = 1,
        })
    end

    local notes = self:getCurrentBookNotes()
    local payload = {
      books = {},
      notes = notes,
      configs = { config }
    }

    client:pushChanges(
        payload,
        function(success, response)
            if interactive then
                if success then
                    UIManager:show(InfoMessage:new{
                        text = _("Book config pushed successfully"),
                        timeout = 2,
                    })
                else
                    UIManager:show(InfoMessage:new{
                        text = _("Failed to push book config"),
                        timeout = 2,
                    })
                end
            end
            if success then
                self.last_sync_timestamp = os.time()
            elseif not interactive then
                self:enqueueSync("push", payload)
            end
        end
    )

end

function ReadestSync:pullBookConfig(interactive)
    if not self.settings.access_token or not self.settings.user_id then
        if interactive then
            UIManager:show(InfoMessage:new{
                text = _("Please login first"),
                timeout = 2,
            })
        end
        return
    end

    local book_hash = self:getDocumentIdentifier()
    local meta_hash = self:getMetaHash()
    if not book_hash or not meta_hash then return end

    -- Only prompt for WiFi if user explicitly requested sync
    -- For auto-sync (non-interactive), silently skip if offline
    if interactive then
        if NetworkMgr:willRerunWhenOnline(function() self:pullBookConfig(interactive) end) then
            return
        end
    elseif not NetworkMgr:isConnected() then
        logger.dbg("ReadestSync: Offline, skipping auto pull")
        return
    end

    local client = self:getReadestSyncClient()
    if not client then
        if interactive then
            UIManager:show(InfoMessage:new{
                text = _("Please configure Readest settings first"),
                timeout = 3,
            })
        end
        return
    end

    self:tryRefreshToken()

    if interactive then
        UIManager:show(InfoMessage:new{
            text = _("Pulling book config..."),
            timeout = 1,
        })
    end

    client:pullChanges(
        {
            since = 0,
            type = "configs",
            book = book_hash,
            meta_hash = meta_hash,
        },
        function(success, response)
            if not success then
                if response and response.error == "Not authenticated" then
                    if interactive then
                        UIManager:show(InfoMessage:new{
                            text = _("Authentication failed, please login again"),
                            timeout = 2,
                        })
                    end
                    self:logout()
                    return
                end
                if interactive then
                    UIManager:show(InfoMessage:new{
                        text = _("Failed to pull book config"),
                        timeout = 2,
                    })
                else
                    self:enqueueSync("pull", {
                        params = {
                            since = 0,
                            type = "configs",
                            book = book_hash,
                            meta_hash = meta_hash,
                        },
                        onSuccess = function(response)
                            local data = response.configs
                            if data and #data > 0 then
                                local config = data[1]
                                if config then
                                    self:applyBookConfig(config)
                                end
                            end
                            local notes = response.notes
                            if notes and #notes > 0 then
                                self:applyNotesToBook(notes)
                            end
                        end
                    })
                end
                return
            end

            local data = response.configs
            if data and #data > 0 then
                local config = data[1]
                if config then
                    self:applyBookConfig(config)
                end
            end

            -- Check for notes and apply them
            local notes = response.notes
            if notes and #notes > 0 then
                self:applyNotesToBook(notes)
            end

            if data and #data > 0 then
                if interactive then
                    UIManager:show(InfoMessage:new{
                        text = _("Book config synchronized"),
                        timeout = 2,
                    })
                end
                return
            end

            if interactive then
                UIManager:show(InfoMessage:new{
                    text = _("No saved config found for this book"),
                    timeout = 2,
                })
            end
        end
    )
end

function ReadestSync:onReadestSyncToggleAutoSync(toggle)
    if toggle == self.settings.auto_sync then
        return true
    end
    self.settings.auto_sync = not self.settings.auto_sync
    G_reader_settings:saveSetting("readest_sync", self.settings)
    if self.settings.auto_sync and self.ui.document then
        self:pullBookConfig(false)
    end
end

function ReadestSync:onReadestSyncPushProgress()
    self:pushBookConfig(true)
end

function ReadestSync:onReadestSyncPullProgress()
    self:pullBookConfig(true)
end

function ReadestSync:onCloseDocument()
    if self.settings.auto_sync and self.settings.access_token then
        -- Only sync if already online, don't prompt for WiFi on document close
        if NetworkMgr:isConnected() then
            self:pushBookConfig(false)
        else
            logger.dbg("ReadestSync: Offline, skipping auto push on close")
        end
    end
end

function ReadestSync:onPageUpdate(page)
    if self.settings.auto_sync and self.settings.access_token and page then
        if self.delayed_push_task then
            UIManager:unschedule(self.delayed_push_task)
        end
        self.delayed_push_task = function()
            self:pushBookConfig(false)
        end
        UIManager:scheduleIn(5, self.delayed_push_task)
    end
end

function ReadestSync:onCloseWidget()
    if self.delayed_push_task then
        UIManager:unschedule(self.delayed_push_task)
        self.delayed_push_task = nil
    end
end

return ReadestSync