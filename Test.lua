setfpscap(5)
game:GetService("RunService"):Set3dRenderingEnabled(false)
local Booths_Broadcast = game:GetService("ReplicatedStorage").Network:WaitForChild("Booths_Broadcast")
local Library = require(game.ReplicatedStorage:WaitForChild('Library'))

if not getgenv().a then
    getgenv().a = true
    local vu = game:GetService("VirtualUser")
    game:GetService("Players").LocalPlayer.Idled:connect(function()
        vu:Button2Down(Vector2.new(0,0),workspace.CurrentCamera.CFrame)
        wait(1)
        vu:Button2Up(Vector2.new(0,0),workspace.CurrentCamera.CFrame)
    end)
end

local function processListingInfo(uid, gems, item, version, shiny, amount, boughtFrom)
    print(uid, gems, item, version, shiny, amount, boughtFrom)
    print("BOUGHT FROM:", boughtFrom)
    print("UID:", uid)
    print("GEMS:", gems)
    print("ITEM:", item)
    local snipeMessage = game.Players.LocalPlayer.Name .. " just sniped a "
    if version then
        if version == 2 then
            version = "Rainbow"
        elseif version == 1 then
            version = "Golden"
        end
    else
       version = "Normal"
    end
    
    snipeMessage = snipeMessage .. version
    
    if shiny then
        snipeMessage = snipeMessage .. " Shiny"
    end
    
    snipeMessage = snipeMessage .. " " .. (item)
    
    print(snipeMessage)
    
    if amount then
        print("AMOUNT:", amount)
    else
        amount = 1
        print("AMOUNT:", amount)
    end

    local fields = {
        {
            name = "PRICE:",
            value = tostring(gems) .. " GEMS",
            inline = true,
        },
        {
            name = "BOUGHT FROM:",
            value = tostring(boughtFrom),
            inline = true,
        },
        {
            name = "AMOUNT:",
            value = tostring(amount),
            inline = true,
        },
        {
            name = "PETID:",
            value = tostring(uid),
            inline = true,
        }
    }

    local message = {
        content = "@everyone",
        embeds = {
            {
                title = snipeMessage,
                fields = fields,
                author = {name = "New Pet Sniped!"}
            }
        },
        username = "piratesniper",
        attachments = {}
    }

    local http = game:GetService("HttpService")
    local jsonMessage = http:JSONEncode(message)

    http:PostAsync(
        "https://discord.com/api/webhooks/1188501479682752615/LCWS4QTpNAG508JKA2osKRtWnYNbem4T43iHJfu4LrCsnyyZrPLvyKq-c5QAJyXeWw7v",
        jsonMessage,
        Enum.HttpContentType.ApplicationJson,
        false
    )
end

local function checklisting(uid, gems, item, version, shiny, amount, username, playerid)
    gems = tonumber(gems)

    local type = {}
    pcall(function()
      type = Library.Directory.Pets[item]
    end)
    
    if type.exclusiveLevel and gems <= 10000 then
    game:GetService("ReplicatedStorage").Network.Booths_RequestPurchase:InvokeServer(playerid, uid)
    processListingInfo(uid, gems, item, version, shiny, amount, username)
    elseif type.huge and gems <= 1000000 then
    game:GetService("ReplicatedStorage").Network.Booths_RequestPurchase:InvokeServer(playerid, uid)
    processListingInfo(uid, gems, item, version, shiny, amount, username)
    elseif type.titanic and gems <= 10000000 then
    game:GetService("ReplicatedStorage").Network.Booths_RequestPurchase:InvokeServer(playerid, uid)
    processListingInfo(uid, gems, item, version, shiny, amount, username)
    end
end

Booths_Broadcast.OnClientEvent:Connect(function(username, message)
    if type(message) == "table" and message['PlayerID'] then
        local playerID = message['PlayerID']
        local listing = message["Listings"]
        for key, value in pairs(listing) do
            if type(value) == "table" then
                local uid = key
                local gems = value["DiamondCost"]
                local itemdata = value["ItemData"]

                if itemdata then
                    local data = itemdata["data"]

                    if data then
                        local item = data["id"]
                        local version = data["pt"]
                        local shiny = data["sh"]
                        local amount = data["_am"]
                        checklisting(uid, gems, item, version, shiny, amount, username , playerID)
                    end
                end
            end
        end
    end
end)
