const { 
    Client, 
    GatewayIntentBits, 
    Partials, 
    Collection, 
    EmbedBuilder,
    ActionRowBuilder,
    ButtonBuilder,
    ButtonStyle,
    ModalBuilder,
    TextInputBuilder,
    TextInputStyle,
    PermissionFlagsBits,
    ChannelType,
    StringSelectMenuBuilder,
    StringSelectMenuOptionBuilder,
    SlashCommandBuilder 
} = require('discord.js');
const sqlite3 = require('sqlite3').verbose();
require('dotenv').config();

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildVoiceStates,
        GatewayIntentBits.DirectMessages,
        GatewayIntentBits.DirectMessageReactions
    ],
    partials: [Partials.Channel, Partials.Message, Partials.User, Partials.GuildMember, Partials.Reaction]
});

const db = new sqlite3.Database('./factions.db');

const pendingFactions = new Map();
const strikeCooldowns = new Map();
const commandCooldowns = new Map();

const STRIKE_COOLDOWN_MS = 86400000;
const COMMAND_COOLDOWN_MS = 3000;

const GLOBAL_HICOMM_ROLE_ID = process.env.HICOMM_ROLE_ID;
const GLOBAL_OWNER_ROLE_ID = process.env.OWNER_ROLE_ID;
const GLOBAL_MEMBER_ROLE_ID = process.env.MEMBER_ROLE_ID;
const GLOBAL_DIRECTOR_ROLE_ID = process.env.DIRECTOR_ROLE_ID;
const GLOBAL_MIDCOMM_ROLE_ID = process.env.MIDCOMM_ROLE_ID;

const channelTypes = [
    { name: '📢〡announcements', type: ChannelType.GuildText, key: 'announcements', permissions: 'readonly' },
    { name: '📚〡documents', type: ChannelType.GuildText, key: 'documents', permissions: 'readonly' },
    { name: '🔰〡deployment', type: ChannelType.GuildText, key: 'deployment', permissions: 'readonly' },
    { name: '💬〡chat', type: ChannelType.GuildText, key: 'chat', permissions: 'readwrite' },
    { name: '🔉〡vc', type: ChannelType.GuildVoice, key: 'voice', permissions: 'voice' },
];

const RANK_ORDER = ['member', 'midcomm', 'hicomm', 'director', 'owner'];

const DEFAULT_PERMISSIONS = {
    'rename': 4,
    'kick': 3,
    'promote': 2,
    'demote': 2,
    'invite': 2,
    'strike': 1,
    'leave': 0,
};

const RANK_NAMES = {
    0: 'Member',
    1: 'Midcomm', 
    2: 'Hicomm',
    3: 'Director',
    4: 'Owner'
};

const PROMOTABLE_RANKS = ['director', 'hicomm', 'midcomm', 'member'];

function getRoleLevelIndex(role) {
    return RANK_ORDER.indexOf(role ? role.toLowerCase() : 'member');
}

function getFactionPermissions(factionId) {
    return new Promise((resolve, reject) => {
        db.get("SELECT * FROM faction_permissions WHERE faction_id = ?", [factionId], (err, row) => {
            if (err) reject(err);
            else {
                if (row) {
                    resolve({
                        rename: row.rename_level,
                        kick: row.kick_level,
                        promote: row.promote_level,
                        demote: row.demote_level,
                        invite: row.invite_level,
                        strike: row.strike_level,
                        leave: row.leave_level
                    });
                } else {
                    resolve(DEFAULT_PERMISSIONS);
                }
            }
        });
    });
}

function updateFactionPermissions(factionId, permissions) {
    return new Promise((resolve, reject) => {
        db.run(`INSERT OR REPLACE INTO faction_permissions 
                (faction_id, rename_level, kick_level, promote_level, demote_level, invite_level, strike_level, leave_level) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [factionId, permissions.rename, permissions.kick, 
             permissions.promote, permissions.demote, permissions.invite, permissions.strike, permissions.leave],
            (err) => {
                if (err) reject(err);
                else resolve();
            });
    });
}

async function checkManagementPermission(factionId, userLevel, action) {
    const permissions = await getFactionPermissions(factionId);
    const requiredLevel = permissions[action] || 5;
    return userLevel >= requiredLevel;
}

function getRankName(level) {
    return RANK_NAMES[level] || 'Owner';
}

function createRankOptions(currentLevel) {
    return [
        { label: 'Member', value: '0', description: 'Lowest rank', default: currentLevel === 0 },
        { label: 'Midcomm', value: '1', description: 'Mid-level command', default: currentLevel === 1 },
        { label: 'Hicomm', value: '2', description: 'High command', default: currentLevel === 2 },
        { label: 'Director', value: '3', description: 'Director level', default: currentLevel === 3 },
        { label: 'Owner', value: '4', description: 'Faction owner only', default: currentLevel === 4 }
    ];
}

function canStrikeUser(strikerLevel, targetLevel) {
    return strikerLevel > targetLevel;
}

function getStrikeCooldownKey(userId, targetId) {
    return `${userId}_${targetId}`;
}

function isOnStrikeCooldown(userId, targetId) {
    const key = getStrikeCooldownKey(userId, targetId);
    const cooldown = strikeCooldowns.get(key);
    if (!cooldown) return false;
    
    if (Date.now() > cooldown) {
        strikeCooldowns.delete(key);
        return false;
    }
    return true;
}

function setStrikeCooldown(userId, targetId) {
    const key = getStrikeCooldownKey(userId, targetId);
    strikeCooldowns.set(key, Date.now() + STRIKE_COOLDOWN_MS);
}

function isOnCommandCooldown(userId, command) {
    const key = `${userId}_${command}`;
    const cooldown = commandCooldowns.get(key);
    if (!cooldown) return false;
    
    if (Date.now() > cooldown) {
        commandCooldowns.delete(key);
        return false;
    }
    return true;
}

function setCommandCooldown(userId, command) {
    const key = `${userId}_${command}`;
    commandCooldowns.set(key, Date.now() + COMMAND_COOLDOWN_MS);
}

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS factions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        unique_id TEXT UNIQUE NOT NULL,
        type TEXT NOT NULL,
        roblox_group TEXT,
        color TEXT NOT NULL,
        image_url TEXT,
        owner_id TEXT NOT NULL,
        role_id TEXT,
        hicomm_role_id TEXT,
        director_role_id TEXT,
        midcomm_role_id TEXT,
        category_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS faction_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faction_id INTEGER,
        channel_type TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        channel_name TEXT NOT NULL,
        FOREIGN KEY(faction_id) REFERENCES factions(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS faction_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faction_id INTEGER,
        user_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(faction_id) REFERENCES factions(id),
        UNIQUE(faction_id, user_id)
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS faction_strikes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faction_id INTEGER NOT NULL,
        user_id TEXT NOT NULL,
        reason TEXT NOT NULL,
        given_by TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(faction_id) REFERENCES factions(id)
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS faction_permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faction_id INTEGER UNIQUE NOT NULL,
        rename_level INTEGER DEFAULT 4,
        kick_level INTEGER DEFAULT 3,
        promote_level INTEGER DEFAULT 2,
        demote_level INTEGER DEFAULT 2,
        invite_level INTEGER DEFAULT 2,
        strike_level INTEGER DEFAULT 1,
        leave_level INTEGER DEFAULT 0,
        FOREIGN KEY(faction_id) REFERENCES factions(id)
    )`);
});

function getFactionByOwner(ownerId) {
    return new Promise((resolve, reject) => {
        db.get("SELECT * FROM factions WHERE owner_id = ?", [ownerId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

function getFactionByMember(userId) {
    return new Promise((resolve, reject) => {
        db.get(`
            SELECT f.*, fm.role as member_role FROM factions f 
            JOIN faction_members fm ON f.id = fm.faction_id 
            WHERE fm.user_id = ?
        `, [userId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

function getFactionChannels(factionId) {
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM faction_channels WHERE faction_id = ?", [factionId], (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}

function getFactionById(factionId) {
    return new Promise((resolve, reject) => {
        db.get("SELECT * FROM factions WHERE id = ?", [factionId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

function getFactionMembers(factionId) {
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM faction_members WHERE faction_id = ?", [factionId], (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}

function getFactionMember(factionId, userId) {
    return new Promise((resolve, reject) => {
        db.get("SELECT * FROM faction_members WHERE faction_id = ? AND user_id = ?", [factionId, userId], (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

function getUserStrikes(factionId, userId) {
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM faction_strikes WHERE faction_id = ? AND user_id = ?", [factionId, userId], (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}

function addStrike(factionId, userId, reason, givenBy) {
    return new Promise((resolve, reject) => {
        db.run("INSERT INTO faction_strikes (faction_id, user_id, reason, given_by) VALUES (?, ?, ?, ?)",
            [factionId, userId, reason, givenBy], function(err) {
                if (err) reject(err);
                else resolve(this.lastID);
            });
    });
}

function removeStrike(strikeId) {
    return new Promise((resolve, reject) => {
        db.run("DELETE FROM faction_strikes WHERE id = ?", [strikeId], (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

function clearAllStrikes(factionId, userId) {
    return new Promise((resolve, reject) => {
        db.run("DELETE FROM faction_strikes WHERE faction_id = ? AND user_id = ?", 
            [factionId, userId], (err) => {
                if (err) reject(err);
                else resolve();
            });
    });
}

async function safeSetNickname(member, nickname) {
    try {
        if (member.guild.members.me.permissions.has(PermissionFlagsBits.ManageNicknames)) {
            await member.setNickname(nickname);
        }
    } catch (error) {}
}

async function logAction(action, description, color = 0x0099FF) {
    try {
        if (!process.env.LOG_CHANNEL_ID) return;
        const logChannel = await client.channels.fetch(process.env.LOG_CHANNEL_ID);
        const embed = new EmbedBuilder()
            .setColor(color)
            .setTitle(`Faction Log - ${action}`)
            .setDescription(description)
            .setTimestamp();
        await logChannel.send({ embeds: [embed] });
    } catch (error) {}
}

client.commands = new Collection();

const commands = [
    new SlashCommandBuilder()
        .setName('create')
        .setDescription('Create a new faction'),
    new SlashCommandBuilder()
        .setName('manage')
        .setDescription('Manage your faction'),
    new SlashCommandBuilder()
        .setName('admin')
        .setDescription('Admin faction management (Admin only)'),
    new SlashCommandBuilder()
        .setName('list')
        .setDescription('List all factions in the server')
];

client.on('ready', async () => {
    console.log(`Logged in as ${client.user.tag}!`);
    try {
        await client.application.commands.set(commands);
        console.log('Slash commands registered!');
    } catch (error) {
        console.error('Error registering commands:', error);
    }
});

async function handleList(interaction) {
    if (isOnCommandCooldown(interaction.user.id, 'list')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'list');
    
    await interaction.deferReply();
    
    const factions = await new Promise((resolve, reject) => {
        db.all("SELECT * FROM factions ORDER BY name", (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    const totalFactions = factions.length;

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('Faction List')
        .setDescription(`${totalFactions} Active Factions`)
        .setFooter({ text: 'Use /manage to manage your faction' })
        .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
}

async function handleAdmin(interaction) {
    if (isOnCommandCooldown(interaction.user.id, 'admin')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'admin');
    
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator)) {
        return interaction.reply({ content: "❌ You need administrator permissions to use this command!", flags: 64 });
    }

    const factions = await new Promise((resolve, reject) => {
        db.all("SELECT * FROM factions ORDER BY name", (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    if (factions.length === 0) {
        return interaction.reply({ content: "❌ No factions found!", flags: 64 });
    }

    const embed = new EmbedBuilder()
        .setColor(0xFF0000)
        .setTitle('🔧 Admin Management')
        .setDescription('Administrative actions for faction management')
        .setFooter({ text: 'Bot reworked by xyn4x' });

    const actionSelectMenu = new StringSelectMenuBuilder()
        .setCustomId('admin_action_select')
        .setPlaceholder('Select an admin action...')
        .addOptions([
            new StringSelectMenuOptionBuilder()
                .setLabel('Force Rename Faction')
                .setValue('force_rename')
                .setDescription('Forcefully rename a faction'),
            new StringSelectMenuOptionBuilder()
                .setLabel('Force Disband Faction')
                .setValue('force_disband')
                .setDescription('Forcefully disband a faction')
        ]);

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(actionSelectMenu)],
        flags: 64
    });
}

async function handleAdminActionSelect(interaction) {
    const action = interaction.values[0];
    
    const factions = await new Promise((resolve, reject) => {
        db.all("SELECT * FROM factions ORDER BY name", (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    if (factions.length === 0) {
        return interaction.update({ content: "❌ No factions found!", components: [] });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`admin_${action}_faction`)
        .setPlaceholder('Select a faction...')
        .addOptions(factions.map(faction => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${faction.name} [${faction.unique_id}]`)
                .setValue(faction.id.toString())
                .setDescription(`Owner: ${faction.owner_id}`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFF0000)
        .setTitle(`🔧 Admin ${action.replace('_', ' ').toUpperCase()}`)
        .setDescription(`Select a faction to ${action.replace('_', ' ')}`)
        .setFooter({ text: 'Administrative action' });

    await interaction.update({ embeds: [embed], components: [new ActionRowBuilder().addComponents(selectMenu)] });
}

async function handleAdminRenameSelect(interaction) {
    const factionId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.update({ content: "❌ Faction not found.", components: [] });
    }
    
    const modal = new ModalBuilder()
        .setCustomId(`admin_rename_${factionId}`)
        .setTitle('Rename Faction');
    
    const nameInput = new TextInputBuilder()
        .setCustomId('new_name')
        .setLabel('New Faction Name')
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setValue(faction.name);
    
    const idInput = new TextInputBuilder()
        .setCustomId('new_id')
        .setLabel('New Faction ID (optional, max 4 letters)')
        .setStyle(TextInputStyle.Short)
        .setMaxLength(4)
        .setRequired(false)
        .setValue(faction.unique_id);
    
    modal.addComponents(
        new ActionRowBuilder().addComponents(nameInput),
        new ActionRowBuilder().addComponents(idInput)
    );
    
    await interaction.showModal(modal);
}

async function handleAdminRenameModal(interaction) {
    const factionId = interaction.customId.replace('admin_rename_', '');
    const newName = interaction.fields.getTextInputValue('new_name');
    const newId = interaction.fields.getTextInputValue('new_id')?.toUpperCase();
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "❌ Faction not found." });
    }
    
    if (newId && newId !== faction.unique_id) {
        const existingFaction = await new Promise((resolve, reject) => {
            db.get("SELECT * FROM factions WHERE unique_id = ?", [newId], (err, row) => {
                if (err) reject(err);
                else resolve(row);
            });
        });
        
        if (existingFaction) {
            return interaction.editReply({ content: `❌ Faction ID **${newId}** is already taken!` });
        }
    }
    
    const guild = interaction.guild;
    
    try {
        if (newId && newId !== faction.unique_id) {
            const factionRole = await guild.roles.fetch(faction.role_id);
            if (factionRole) {
                await factionRole.setName(`[${newId}]`);
            }
            
            const category = await guild.channels.fetch(faction.category_id);
            if (category) {
                await category.setName(`〡${newId}`);
            }
            
            const factionMembers = await getFactionMembers(factionId);
            for (const memberData of factionMembers) {
                try {
                    const member = await guild.members.fetch(memberData.user_id);
                    await safeSetNickname(member, `[${newId}] ${member.user.username}`);
                } catch (error) {}
            }
        }
        
        const updateQuery = newId ? 
            "UPDATE factions SET name = ?, unique_id = ? WHERE id = ?" :
            "UPDATE factions SET name = ? WHERE id = ?";
        
        const updateParams = newId ? 
            [newName, newId, factionId] :
            [newName, factionId];
        
        db.run(updateQuery, updateParams, async (err) => {
            if (err) {
                console.error(err);
                return interaction.editReply({ content: "❌ Database error while renaming faction." });
            }
            
            const successEmbed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Faction Renamed')
                .setDescription(`Successfully renamed faction`)
                .addFields(
                    { name: 'Old Name', value: faction.name, inline: true },
                    { name: 'New Name', value: newName, inline: true }
                );
            
            if (newId && newId !== faction.unique_id) {
                successEmbed.addFields(
                    { name: 'Old ID', value: faction.unique_id, inline: true },
                    { name: 'New ID', value: newId, inline: true }
                );
            }
            
            successEmbed.addFields(
                { name: 'Renamed By', value: `<@${interaction.user.id}>`, inline: true }
            );
            
            await interaction.editReply({ embeds: [successEmbed] });
            await logAction('FACTION_RENAMED', 
                `<@${interaction.user.id}> renamed **${faction.name} [${faction.unique_id}]** to **${newName} [${newId || faction.unique_id}]**`, 
                0x00FF00);
        });
        
    } catch (error) {
        console.error('Error renaming faction:', error);
        await interaction.editReply({ content: "❌ Error renaming faction." });
    }
}

async function handleAdminDisbandSelect(interaction) {
    const factionId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.update({ content: "❌ Faction not found.", components: [] });
    }
    
    const confirmEmbed = new EmbedBuilder()
        .setColor(0xFF0000)
        .setTitle('⚠️ Force Disband Faction')
        .setDescription(`Are you sure you want to force disband **${faction.name} [${faction.unique_id}]**?`)
        .addFields(
            { name: 'Faction Owner', value: `<@${faction.owner_id}>`, inline: true },
            { name: 'Faction Type', value: faction.type, inline: true },
            { name: 'Members', value: 'All members will be removed', inline: true }
        )
        .setFooter({ text: 'This action is irreversible and will delete all faction data!' });
    
    const buttons = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`admin_confirm_disband_${factionId}`)
            .setLabel('Yes, Disband')
            .setStyle(ButtonStyle.Danger),
        new ButtonBuilder()
            .setCustomId('admin_cancel_disband')
            .setLabel('Cancel')
            .setStyle(ButtonStyle.Secondary)
    );
    
    await interaction.update({ embeds: [confirmEmbed], components: [buttons] });
}

async function handleAdminConfirmDisband(interaction) {
    const factionId = interaction.customId.replace('admin_confirm_disband_', '');
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "❌ Faction not found.", components: [] });
    }
    
    try {
        const guild = interaction.guild;
        
        const factionMembers = await getFactionMembers(factionId);
        
        try {
            const category = await guild.channels.fetch(faction.category_id);
            if (category) {
                const channels = await getFactionChannels(factionId);
                for (const channelData of channels) {
                    try {
                        const channel = await guild.channels.fetch(channelData.channel_id);
                        if (channel) await channel.delete();
                    } catch (error) {}
                }
                await category.delete();
            }
            
            const [factionRole, ownerRole, memberRole, midcommRole, hiCommRole, directorRole] = await Promise.all([
                guild.roles.fetch(faction.role_id),
                guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID),
                guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID)
            ]);
            
            for (const memberData of factionMembers) {
                try {
                    const member = await guild.members.fetch(memberData.user_id);
                    
                    if (factionRole && member.roles.cache.has(factionRole.id)) {
                        await member.roles.remove(factionRole.id);
                    }
                    
                    if (memberData.role.toLowerCase() === 'owner' && ownerRole && member.roles.cache.has(ownerRole.id)) {
                        await member.roles.remove(ownerRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'director' && directorRole && member.roles.cache.has(directorRole.id)) {
                        await member.roles.remove(directorRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'hicomm' && hiCommRole && member.roles.cache.has(hiCommRole.id)) {
                        await member.roles.remove(hiCommRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'midcomm' && midcommRole && member.roles.cache.has(midcommRole.id)) {
                        await member.roles.remove(midcommRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'member' && memberRole && member.roles.cache.has(memberRole.id)) {
                        await member.roles.remove(memberRole.id);
                    }
                    
                    await safeSetNickname(member, null);
                    
                } catch (error) {}
            }
            
            if (factionRole) await factionRole.delete();
            
        } catch (discordError) {
            console.error('Error deleting Discord resources:', discordError);
        }
        
        await new Promise((resolve, reject) => {
            db.serialize(() => {
                db.run("DELETE FROM faction_strikes WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_members WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_channels WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_permissions WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM factions WHERE id = ?", [factionId], function(err) {
                    if (err) reject(err);
                    else resolve();
                });
            });
        });
        
        const successEmbed = new EmbedBuilder()
            .setColor(0xFF0000)
            .setTitle('✅ Faction Disbanded')
            .setDescription(`Successfully force disbanded **${faction.name} [${faction.unique_id}]**`)
            .addFields(
                { name: 'Former Owner', value: `<@${faction.owner_id}>`, inline: true },
                { name: 'Disbanded By', value: `<@${interaction.user.id}>`, inline: true },
                { name: 'Faction Type', value: faction.type, inline: true },
                { name: 'Members Affected', value: `${factionMembers.length} members`, inline: true }
            )
            .setFooter({ text: 'All faction data has been permanently deleted and roles removed' });
        
        await interaction.editReply({ embeds: [successEmbed], components: [] });
        await logAction('FACTION_DISBANDED', 
            `<@${interaction.user.id}> force disbanded **${faction.name} [${faction.unique_id}]** (Owner: <@${faction.owner_id}>, Members: ${factionMembers.length})`, 
            0xFF0000); 
            
    } catch (error) {
        console.error('Error disbanding faction:', error);
        await interaction.editReply({ content: "❌ Error disbanding faction.", components: [] });
    }
}

async function handleAdminCancelDisband(interaction) {
    await interaction.update({ 
        content: "❌ Faction disband cancelled.", 
        components: [], 
        embeds: [] 
    });
}

async function handleCreate(interaction) {
    if (isOnCommandCooldown(interaction.user.id, 'create')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'create');
    
    const existingFaction = await getFactionByOwner(interaction.user.id);
    if (existingFaction) {
        return interaction.reply({ content: "You already own a faction!", flags: 64 });
    }

    const modal = new ModalBuilder()
        .setCustomId('create_faction')
        .setTitle('Create Faction');
    
    const nameInput = new TextInputBuilder()
        .setCustomId('faction_name').setLabel('Faction Name').setStyle(TextInputStyle.Short).setRequired(true);
    const idInput = new TextInputBuilder()
        .setCustomId('faction_id').setLabel('Faction ID (max 4 letters)').setStyle(TextInputStyle.Short).setMaxLength(4).setRequired(true);
    const typeInput = new TextInputBuilder()
        .setCustomId('faction_type').setLabel('Faction Type').setStyle(TextInputStyle.Short).setRequired(true);
    const groupInput = new TextInputBuilder()
        .setCustomId('roblox_group').setLabel('Roblox Group Link').setStyle(TextInputStyle.Short).setRequired(true);
    const colorInput = new TextInputBuilder()
        .setCustomId('faction_color').setLabel('Faction Color (Hex)').setStyle(TextInputStyle.Short).setRequired(true);

    modal.addComponents(
        new ActionRowBuilder().addComponents(nameInput),
        new ActionRowBuilder().addComponents(idInput),
        new ActionRowBuilder().addComponents(typeInput),
        new ActionRowBuilder().addComponents(groupInput),
        new ActionRowBuilder().addComponents(colorInput)
    );
    await interaction.showModal(modal);
}

async function handleManage(interaction) {
    if (isOnCommandCooldown(interaction.user.id, 'manage')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'manage');
    
    const faction = await getFactionByMember(interaction.user.id);
    if (!faction) return interaction.reply({ content: "You're not in any faction!", flags: 64 });

    const userRank = faction.member_role.toLowerCase();
    const userLevel = getRoleLevelIndex(userRank);
    if (userLevel === 0) return interaction.reply({ content: "Only Midcomm and above can access management.", flags: 64 });

    const availableActions = [];
    
    if (await checkManagementPermission(faction.id, userLevel, 'rename')) availableActions.push({ label: '🏷️ Rename Faction', value: 'rename', description: 'Change faction name' });
    if (await checkManagementPermission(faction.id, userLevel, 'promote')) availableActions.push({ label: '⬆️ Promote Member', value: 'promote', description: 'Promote a member' });
    if (await checkManagementPermission(faction.id, userLevel, 'demote')) availableActions.push({ label: '⬇️ Demote Member', value: 'demote', description: 'Demote a member' });
    if (await checkManagementPermission(faction.id, userLevel, 'invite')) availableActions.push({ label: '📩 Invite Members', value: 'invite', description: 'Invite multiple members' });
    if (await checkManagementPermission(faction.id, userLevel, 'kick')) availableActions.push({ label: '👢 Kick Member', value: 'kick', description: 'Remove a member' });
    if (await checkManagementPermission(faction.id, userLevel, 'strike')) availableActions.push({ label: '⚡ Give Strike', value: 'strike', description: 'Give a strike to member' });
    
    availableActions.push({ label: '📋 View Strikes', value: 'view_strikes', description: 'View member strikes' });
    
    if (userLevel >= 3) {
        availableActions.push({ label: '🗑️ Remove Strike', value: 'remove_strike', description: 'Remove strikes from members' });
    }

    if (userRank === 'owner') {
        availableActions.push({ label: '👑 Transfer Ownership', value: 'transfer', description: 'Transfer faction ownership' });
        availableActions.push({ label: '💥 Disband Faction', value: 'disband', description: 'Permanently delete faction' });
        availableActions.push({ label: '⚙️ Faction Settings', value: 'settings', description: 'Configure faction permissions' });
        availableActions.push({ label: '➕ Add Channel', value: 'add_channel', description: 'Add a new channel to faction' });
        availableActions.push({ label: '🗑️ Remove Channel', value: 'remove_channel', description: 'Remove a channel from faction' });
        availableActions.push({ label: '✏️ Rename Channel', value: 'rename_channel', description: 'Rename a faction channel' });
    }

    availableActions.push({ label: '🚪 Leave Faction', value: 'leave', description: 'Leave the faction' });

    if (availableActions.length === 0) return interaction.reply({ content: "No permissions available.", flags: 64 });

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId('manage_select')
        .setPlaceholder('Select a management action...')
        .addOptions(availableActions.map(action => 
            new StringSelectMenuOptionBuilder()
                .setLabel(action.label)
                .setValue(action.value)
                .setDescription(action.description)
        ));

    const embed = new EmbedBuilder()
        .setColor(faction.color || 0x2b2d31)
        .setTitle(`${faction.name} Management`)
        .setDescription(`Your Rank: \`${userRank.toUpperCase()}\`\nFaction ID: \`${faction.unique_id}\``)
        .setFooter({ text: 'Bot reworked by xyn4x' })
        .setTimestamp();

    await interaction.reply({ embeds: [embed], components: [new ActionRowBuilder().addComponents(selectMenu)], flags: 64 });
}

async function handleStrikeMember(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'strike')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'strike');
    
    const members = await getFactionMembers(faction.id);
    const strikerLevel = getRoleLevelIndex(faction.member_role);
    
    const strikeableMembers = members.filter(member => {
        const targetLevel = getRoleLevelIndex(member.role);
        return member.user_id !== interaction.user.id && canStrikeUser(strikerLevel, targetLevel);
    });

    if (strikeableMembers.length === 0) {
        return interaction.reply({ content: "❌ No members available to strike.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`strike_select_${faction.id}`)
        .setPlaceholder('Select member to strike...')
        .addOptions(strikeableMembers.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`Strike this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('⚡ Strike Member')
        .setDescription('Select a member to give a strike:')
        .setFooter({ text: 'You can only strike members lower than your rank' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleStrikeSelect(interaction) {
    const factionId = interaction.customId.replace('strike_select_', '');
    const targetId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    const strikerMember = await getFactionMember(factionId, interaction.user.id);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!strikerMember || !targetMember) {
        return interaction.reply({ content: "❌ Member not found.", flags: 64 });
    }

    const strikerLevel = getRoleLevelIndex(strikerMember.role);
    const targetLevel = getRoleLevelIndex(targetMember.role);
    
    if (!canStrikeUser(strikerLevel, targetLevel)) {
        return interaction.reply({ content: "❌ You cannot strike this member.", flags: 64 });
    }

    if (isOnStrikeCooldown(interaction.user.id, targetId)) {
        return interaction.reply({ content: "⏰ You can only strike this member once per 24 hours.", flags: 64 });
    }

    const modal = new ModalBuilder()
        .setCustomId(`strike_reason_${factionId}_${targetId}`)
        .setTitle('Give Strike');

    const reasonInput = new TextInputBuilder()
        .setCustomId('strike_reason')
        .setLabel('Strike Reason')
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setPlaceholder('Enter the reason for this strike...')
        .setMaxLength(500);

    modal.addComponents(new ActionRowBuilder().addComponents(reasonInput));
    await interaction.showModal(modal);
}

async function handleStrikeReasonModal(interaction) {
    const [factionId, targetId] = interaction.customId.replace('strike_reason_', '').split('_');
    const reason = interaction.fields.getTextInputValue('strike_reason');
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!faction || !targetMember) {
        return interaction.editReply({ content: "❌ Faction or member not found." });
    }

    try {
        await addStrike(factionId, targetId, reason, interaction.user.id);
        setStrikeCooldown(interaction.user.id, targetId);
        
        const strikes = await getUserStrikes(factionId, targetId);
        const strikeCount = strikes.length;

        const successEmbed = new EmbedBuilder()
            .setColor(0xFFA500)
            .setTitle('⚡ Strike Given')
            .setDescription(`Strike given to <@${targetId}>`)
            .addFields(
                { name: 'Reason', value: reason, inline: false },
                { name: 'Total Strikes', value: `${strikeCount} strikes`, inline: true },
                { name: 'Given By', value: `<@${interaction.user.id}>`, inline: true }
            )
            .setFooter({ text: 'Strike recorded successfully' });

        await interaction.editReply({ embeds: [successEmbed] });
        
        try {
            const targetUser = await client.users.fetch(targetId);
            const strikeEmbed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('⚡ You Received a Strike')
                .setDescription(`You have received a strike in **${faction.name}**`)
                .addFields(
                    { name: 'Reason', value: reason, inline: false },
                    { name: 'Total Strikes', value: `${strikeCount} strikes`, inline: true },
                    { name: 'Given By', value: `<@${interaction.user.id}>`, inline: true }
                )
                .setFooter({ text: 'Please follow faction rules to avoid further strikes' });
            
            await targetUser.send({ embeds: [strikeEmbed] });
        } catch (dmError) {
            console.log('Could not send DM to user');
        }
        
        await logAction('STRIKE_GIVEN', 
            `<@${interaction.user.id}> gave a strike to <@${targetId}> in **${faction.name}**\n**Reason:** ${reason}\n**Total Strikes:** ${strikeCount}`, 
            0xFFA500);

    } catch (error) {
        console.error('Error giving strike:', error);
        await interaction.editReply({ content: "❌ Error giving strike." });
    }
}

async function handleViewStrikes(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'view_strikes')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'view_strikes');
    
    const members = await getFactionMembers(faction.id);
    
    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`view_strikes_${faction.id}`)
        .setPlaceholder('Select member to view strikes...')
        .addOptions(members.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`View strikes for this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('📋 View Member Strikes')
        .setDescription('Select a member to view their strikes:');

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleViewStrikesSelect(interaction) {
    const factionId = interaction.customId.replace('view_strikes_', '');
    const targetId = interaction.values[0];
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    const strikes = await getUserStrikes(factionId, targetId);
    
    if (!faction || !targetMember) {
        return interaction.editReply({ content: "❌ Member not found." });
    }

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle(`📋 Strikes for <@${targetId}>`)
        .setDescription(`**Total Strikes:** ${strikes.length}`)
        .setFooter({ text: `Faction: ${faction.name}` });

    if (strikes.length === 0) {
        embed.setDescription('This member has no strikes.');
    } else {
        strikes.slice(0, 10).forEach((strike, index) => {
            embed.addFields({
                name: `Strike #${index + 1}`,
                value: `**Reason:** ${strike.reason}\n**Given By:** <@${strike.given_by}>\n**Date:** <t:${Math.floor(new Date(strike.created_at).getTime() / 1000)}:R>`,
                inline: false
            });
        });
        
        if (strikes.length > 10) {
            embed.addFields({
                name: 'Note',
                value: `Showing 10 out of ${strikes.length} strikes`,
                inline: false
            });
        }
    }

    await interaction.editReply({ embeds: [embed] });
}

async function handleRemoveStrike(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'remove_strike')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'remove_strike');
    
    const userLevel = getRoleLevelIndex(faction.member_role);
    if (userLevel < 3) {
        return interaction.reply({ content: "❌ Only Director and above can remove strikes.", flags: 64 });
    }

    const members = await getFactionMembers(faction.id);
    const membersWithStrikes = [];
    
    for (const member of members) {
        const strikes = await getUserStrikes(faction.id, member.user_id);
        if (strikes.length > 0) {
            membersWithStrikes.push({
                ...member,
                strikeCount: strikes.length
            });
        }
    }

    if (membersWithStrikes.length === 0) {
        return interaction.reply({ content: "❌ No members have strikes to remove.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`remove_strike_select_${faction.id}`)
        .setPlaceholder('Select member to remove strikes...')
        .addOptions(membersWithStrikes.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`${member.strikeCount} strike(s)`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('🗑️ Remove Strikes')
        .setDescription('Select a member to remove their strikes:');

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleRemoveStrikeSelect(interaction) {
    const factionId = interaction.customId.replace('remove_strike_select_', '');
    const targetId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    const strikes = await getUserStrikes(factionId, targetId);
    
    if (strikes.length === 0) {
        return interaction.reply({ content: "❌ This member has no strikes to remove.", flags: 64 });
    }

    const embed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('🗑️ Remove Strikes')
        .setDescription(`Select which strike to remove from <@${targetId}>:`)
        .setFooter({ text: `Total strikes: ${strikes.length}` });

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`remove_strike_confirm_${factionId}_${targetId}`)
        .setPlaceholder('Select strike to remove...')
        .addOptions(strikes.map((strike, index) => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`Strike #${index + 1}`)
                .setValue(strike.id.toString())
                .setDescription(strike.reason.length > 50 ? strike.reason.substring(0, 47) + '...' : strike.reason)
        ));

    await interaction.update({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)]
    });
}

async function handleRemoveStrikeConfirm(interaction) {
    const [factionId, targetId] = interaction.customId.replace('remove_strike_confirm_', '').split('_');
    const strikeId = interaction.values[0];
    
    await interaction.deferUpdate();
    
    try {
        await removeStrike(strikeId);
        
        const remainingStrikes = await getUserStrikes(factionId, targetId);
        const faction = await getFactionById(factionId);
        
        const successEmbed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Strike Removed')
            .setDescription(`Strike removed from <@${targetId}>`)
            .addFields(
                { name: 'Remaining Strikes', value: `${remainingStrikes.length} strikes`, inline: true },
                { name: 'Removed By', value: `<@${interaction.user.id}>`, inline: true }
            )
            .setFooter({ text: 'Strike removed successfully' });

        await interaction.editReply({ embeds: [successEmbed], components: [] });
        
        await logAction('STRIKE_REMOVED', 
            `<@${interaction.user.id}> removed a strike from <@${targetId}> in **${faction.name}**\n**Remaining Strikes:** ${remainingStrikes.length}`, 
            0x00FF00);

    } catch (error) {
        console.error('Error removing strike:', error);
        await interaction.editReply({ content: "❌ Error removing strike.", components: [] });
    }
}

async function handleFactionSettings(interaction, faction) {
    const currentPermissions = await getFactionPermissions(faction.id);
    
    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('⚙️ Faction Permission Settings')
        .setDescription('Configure the minimum rank required for each action:\n\n**Rank Order:** Owner (4) → Director (3) → Hicomm (2) → Midcomm (1) → Member (0)')
        .addFields(
            { name: '🛠️ Management Actions', value: `• **Rename Faction:** ${getRankName(currentPermissions.rename)}\n• **Kick Members:** ${getRankName(currentPermissions.kick)}`, inline: true },
            { name: '👥 Member Management', value: `• **Promote Members:** ${getRankName(currentPermissions.promote)}\n• **Demote Members:** ${getRankName(currentPermissions.demote)}\n• **Invite Members:** ${getRankName(currentPermissions.invite)}\n• **Give Strikes:** ${getRankName(currentPermissions.strike)}`, inline: true },
            { name: '🔐 Basic Permissions', value: `• **Leave Faction:** ${getRankName(currentPermissions.leave)}`, inline: false }
        )
        .setFooter({ text: 'Only faction owners can modify these settings' });

    const buttons = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`settings_edit_${faction.id}`)
            .setLabel('Edit Permissions')
            .setStyle(ButtonStyle.Primary),
        new ButtonBuilder()
            .setCustomId(`settings_reset_${faction.id}`)
            .setLabel('Reset to Default')
            .setStyle(ButtonStyle.Secondary),
        new ButtonBuilder()
            .setCustomId('settings_cancel')
            .setLabel('Cancel')
            .setStyle(ButtonStyle.Danger)
    );

    await interaction.reply({ embeds: [embed], components: [buttons], flags: 64 });
}

async function handleSettingsEdit(interaction, factionId) {
    const currentPermissions = await getFactionPermissions(factionId);
    
    const embed = new EmbedBuilder()
        .setColor(0x9B59B6)
        .setTitle('🔧 Edit Faction Permissions')
        .setDescription('Select which permission level you want to modify:')
        .addFields(
            { name: 'Current Settings', value: `Rename: **${getRankName(currentPermissions.rename)}** | Kick: **${getRankName(currentPermissions.kick)}**\nPromote: **${getRankName(currentPermissions.promote)}** | Demote: **${getRankName(currentPermissions.demote)}**\nInvite: **${getRankName(currentPermissions.invite)}** | Strike: **${getRankName(currentPermissions.strike)}** | Leave: **${getRankName(currentPermissions.leave)}**`, inline: false }
        )
        .setFooter({ text: 'Select an action to modify its permission level' });

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`settings_select_action_${factionId}`)
        .setPlaceholder('Select permission to edit...')
        .addOptions([
            new StringSelectMenuOptionBuilder().setLabel('Rename Faction').setValue('rename').setDescription(`Current: ${getRankName(currentPermissions.rename)}`),
            new StringSelectMenuOptionBuilder().setLabel('Kick Members').setValue('kick').setDescription(`Current: ${getRankName(currentPermissions.kick)}`),
            new StringSelectMenuOptionBuilder().setLabel('Promote Members').setValue('promote').setDescription(`Current: ${getRankName(currentPermissions.promote)}`),
            new StringSelectMenuOptionBuilder().setLabel('Demote Members').setValue('demote').setDescription(`Current: ${getRankName(currentPermissions.demote)}`),
            new StringSelectMenuOptionBuilder().setLabel('Invite Members').setValue('invite').setDescription(`Current: ${getRankName(currentPermissions.invite)}`),
            new StringSelectMenuOptionBuilder().setLabel('Give Strikes').setValue('strike').setDescription(`Current: ${getRankName(currentPermissions.strike)}`),
            new StringSelectMenuOptionBuilder().setLabel('Leave Faction').setValue('leave').setDescription(`Current: ${getRankName(currentPermissions.leave)}`)
        ]);

    await interaction.update({ embeds: [embed], components: [new ActionRowBuilder().addComponents(selectMenu)] });
}

async function handleSettingsSelectAction(interaction, factionId) {
    const action = interaction.values[0];
    const currentPermissions = await getFactionPermissions(factionId);
    const currentLevel = currentPermissions[action];
    
    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle(`🔧 Set ${action.charAt(0).toUpperCase() + action.slice(1)} Permission`)
        .setDescription(`Current requirement: **${getRankName(currentLevel)}**\n\nSelect the minimum rank required for this action:`)
        .setFooter({ text: 'Higher numbers = more restrictive (Owner only = 4)' });

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`settings_set_level_${factionId}_${action}`)
        .setPlaceholder('Select minimum rank...')
        .addOptions(createRankOptions(currentLevel));

    await interaction.update({ embeds: [embed], components: [new ActionRowBuilder().addComponents(selectMenu)] });
}

async function handleSettingsSetLevel(interaction, factionId, action) {
    const newLevel = parseInt(interaction.values[0]);
    const currentPermissions = await getFactionPermissions(factionId);
    
    currentPermissions[action] = newLevel;
    
    await updateFactionPermissions(factionId, currentPermissions);
    
    const embed = new EmbedBuilder()
        .setColor(0x00FF00)
        .setTitle('✅ Permission Updated')
        .setDescription(`**${action.charAt(0).toUpperCase() + action.slice(1)}** permission level has been updated!`)
        .addFields(
            { name: 'New Requirement', value: getRankName(newLevel), inline: true },
            { name: 'Updated By', value: `<@${interaction.user.id}>`, inline: true }
        )
        .setFooter({ text: 'Changes saved successfully' });

    await interaction.update({ embeds: [embed], components: [] });
    await logAction('PERMISSIONS_UPDATED', `<@${interaction.user.id}> updated ${action} permission to ${getRankName(newLevel)} in **${factionId}**`, 0x00FF00);
}

async function handleSettingsReset(interaction, factionId) {
    await updateFactionPermissions(factionId, DEFAULT_PERMISSIONS);
    
    const embed = new EmbedBuilder()
        .setColor(0x00FF00)
        .setTitle('✅ Permissions Reset')
        .setDescription('All faction permissions have been reset to default values!')
        .addFields(
            { name: 'Default Settings', value: `Rename: **Owner** | Kick: **Director**\nPromote: **Hicomm** | Demote: **Hicomm**\nInvite: **Hicomm** | Strike: **Midcomm** | Leave: **Member**`, inline: false }
        )
        .setFooter({ text: 'Permissions restored to factory defaults' });

    await interaction.update({ embeds: [embed], components: [] });
    await logAction('PERMISSIONS_RESET', `<@${interaction.user.id}> reset all permissions to default in **${factionId}**`, 0x00FF00);
}

async function handleSettingsCancel(interaction) {
    await interaction.update({ 
        content: "❌ Settings cancelled.", 
        components: [], 
        embeds: [] 
    });
}

async function handleAddNewChannel(interaction, faction) {
    const modal = new ModalBuilder()
        .setCustomId(`add_channel_${faction.id}`)
        .setTitle('Add New Channel');

    const nameInput = new TextInputBuilder()
        .setCustomId('channel_name')
        .setLabel('Channel Name')
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setPlaceholder('e.g., general, announcements, commands');

    modal.addComponents(new ActionRowBuilder().addComponents(nameInput));

    await interaction.showModal(modal);
}

async function handleRemoveChannel(interaction, faction) {
    const channels = await getFactionChannels(faction.id);
    
    if (channels.length === 0) {
        return interaction.reply({ content: "No channels found to remove.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`remove_channel_${faction.id}`)
        .setPlaceholder('Select channel to remove...')
        .addOptions(channels.map(channel => 
            new StringSelectMenuOptionBuilder()
                .setLabel(channel.channel_name)
                .setValue(channel.channel_id)
                .setDescription(`ID: ${channel.channel_id}`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFF0000)
        .setTitle('Remove Channel')
        .setDescription('Select a channel to remove from your faction:')
        .setFooter({ text: 'This action cannot be undone!' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleRenameChannel(interaction, faction) {
    const channels = await getFactionChannels(faction.id);
    
    if (channels.length === 0) {
        return interaction.reply({ content: "No channels found to rename.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`rename_channel_select_${faction.id}`)
        .setPlaceholder('Select channel to rename...')
        .addOptions(channels.map(channel => 
            new StringSelectMenuOptionBuilder()
                .setLabel(channel.channel_name)
                .setValue(channel.channel_id)
                .setDescription(`Current: ${channel.channel_name}`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('Rename Channel')
        .setDescription('Select a channel to rename:');

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleRemoveChannelSelect(interaction) {
    const factionId = interaction.customId.replace('remove_channel_', '');
    const channelId = interaction.values[0];
    
    await interaction.deferUpdate();
    
    try {
        const guild = interaction.guild;
        const channel = await guild.channels.fetch(channelId).catch(() => null);
        
        if (!channel) {
            db.run("DELETE FROM faction_channels WHERE channel_id = ?", [channelId]);
            return interaction.followUp({ content: "Channel already deleted, removed from database.", flags: 64 });
        }

        await channel.delete();

        db.run("DELETE FROM faction_channels WHERE channel_id = ?", [channelId], async (err) => {
            if (err) {
                console.error(err);
                return interaction.followUp({ content: "Database error while removing channel.", flags: 64 });
            }

            const successEmbed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('Channel Removed')
                .setDescription(`Successfully removed channel: ${channel.name}`)
                .addFields(
                    { name: 'Channel Type', value: channel.type === ChannelType.GuildText ? 'Text' : 'Voice', inline: true },
                    { name: 'Removed By', value: `<@${interaction.user.id}>`, inline: true }
                );

            try {
                await interaction.editReply({ embeds: [successEmbed], components: [] });
            } catch (editError) {
                await interaction.followUp({ embeds: [successEmbed], flags: 64 });
            }
            await logAction('CHANNEL_REMOVED', `<@${interaction.user.id}> removed channel ${channel.name} from faction`, 0xFF0000);
        });

    } catch (error) {
        console.error('Channel removal error:', error);
        await interaction.followUp({ content: "Error removing channel.", flags: 64 });
    }
}

async function handleRenameChannelSelect(interaction) {
    const factionId = interaction.customId.replace('rename_channel_select_', '');
    const channelId = interaction.values[0];
    
    const modal = new ModalBuilder()
        .setCustomId(`rename_channel_modal_${factionId}_${channelId}`)
        .setTitle('Rename Channel');

    const nameInput = new TextInputBuilder()
        .setCustomId('new_name')
        .setLabel('New Channel Name')
        .setStyle(TextInputStyle.Short)
        .setRequired(true);

    modal.addComponents(new ActionRowBuilder().addComponents(nameInput));

    await interaction.showModal(modal);
}

async function handleRenameChannelModal(interaction, factionId, channelId) {
    const newName = interaction.fields.getTextInputValue('new_name');
    
    await interaction.deferReply({ flags: 64 });
    
    try {
        const guild = interaction.guild;
        const channel = await guild.channels.fetch(channelId).catch(() => null);
        
        if (!channel) {
            return interaction.editReply({ content: "Channel not found." });
        }

        const oldName = channel.name;
        
        await channel.setName(newName);

        db.run("UPDATE faction_channels SET channel_name = ? WHERE channel_id = ?", [newName, channelId], async (err) => {
            if (err) {
                console.error(err);
                return interaction.editReply({ content: "Database error while renaming channel." });
            }

            const successEmbed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('Channel Renamed')
                .setDescription(`Successfully renamed channel`)
                .addFields(
                    { name: 'Old Name', value: oldName, inline: true },
                    { name: 'New Name', value: newName, inline: true },
                    { name: 'Renamed By', value: `<@${interaction.user.id}>`, inline: true }
                );

            await interaction.editReply({ embeds: [successEmbed] });
            await logAction('CHANNEL_RENAMED', `<@${interaction.user.id}> renamed channel from ${oldName} to ${newName}`, 0x3498DB);
        });

    } catch (error) {
        console.error('Channel rename error:', error);
        await interaction.editReply({ content: "Error renaming channel." });
    }
}

async function handleAddChannelModal(interaction, factionId) {
    const channelName = interaction.fields.getTextInputValue('channel_name');
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "Faction not found." });
    }

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('🔧 Select Channel Type')
        .setDescription(`Setting up channel: **${channelName}**`)
        .addFields(
            { name: 'Channel Name', value: channelName, inline: true },
            { name: 'Faction', value: faction.name, inline: true }
        );

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`channel_type_select_${factionId}_${Buffer.from(channelName).toString('base64')}`)
        .setPlaceholder('Select channel type...')
        .addOptions([
            new StringSelectMenuOptionBuilder()
                .setLabel('💬 Text Channel')
                .setValue('text')
                .setDescription('Create a text channel'),
            new StringSelectMenuOptionBuilder()
                .setLabel('🔊 Voice Channel')
                .setValue('voice')
                .setDescription('Create a voice channel')
        ]);

    await interaction.editReply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)]
    });
}

async function handleChannelTypeSelect(interaction) {
    const [factionId, channelNameBase64] = interaction.customId.replace('channel_type_select_', '').split('_');
    const channelType = interaction.values[0];
    const channelName = Buffer.from(channelNameBase64, 'base64').toString();
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.followUp({ content: "Faction not found.", flags: 64 });
    }

    const embed = new EmbedBuilder()
        .setColor(0x3498DB)
        .setTitle('🔧 Configure Channel Permissions')
        .setDescription(`Setting up **${channelType}** channel: **${channelName}**`)
        .addFields(
            { name: 'Channel Name', value: channelName, inline: true },
            { name: 'Channel Type', value: channelType, inline: true },
            { name: 'Faction', value: faction.name, inline: true }
        );

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`channel_perms_${factionId}_${channelType}_${Buffer.from(channelName).toString('base64')}_${Buffer.from('custom').toString('base64')}`)
        .setPlaceholder('Select permissions configuration...')
        .addOptions([
            new StringSelectMenuOptionBuilder().setLabel('👑 Owner + Director Only').setValue('owner_director_only').setDescription('Only Owner and Director can access'),
            new StringSelectMenuOptionBuilder().setLabel('⭐ Hicomm+ (Read & Write)').setValue('hicomm_readwrite').setDescription('Hicomm and above can view and send'),
            new StringSelectMenuOptionBuilder().setLabel('🔷 Midcomm+ (Read & Write)').setValue('midcomm_readwrite').setDescription('Midcomm and above can view and send'),
            new StringSelectMenuOptionBuilder().setLabel('👥 All Members (Read & Write)').setValue('all_readwrite').setDescription('All faction members can view and send'),
            new StringSelectMenuOptionBuilder().setLabel('📢 Announcements (Read Only)').setValue('announcements').setDescription('All can view, only Hicomm+ can send'),
            new StringSelectMenuOptionBuilder().setLabel('🎙️ Voice Channel (All Members)').setValue('voice_all').setDescription('All members can join and speak'),
            new StringSelectMenuOptionBuilder().setLabel('🎙️ Voice Channel (Midcomm+)').setValue('voice_midcomm').setDescription('Midcomm and above can join and speak'),
        ]);

    await interaction.editReply({ embeds: [embed], components: [new ActionRowBuilder().addComponents(selectMenu)] });
}

async function handleChannelPermissionSelect(interaction) {
    const [factionId, type, nameBase64, keyBase64] = interaction.customId.replace('channel_perms_', '').split('_');
    const permissionType = interaction.values[0];
    
    const name = Buffer.from(nameBase64, 'base64').toString();
    const key = Buffer.from(keyBase64, 'base64').toString();
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "❌ Faction not found." });
    }

    const guild = interaction.guild;
    const category = await guild.channels.fetch(faction.category_id);
    if (!category) {
        return interaction.editReply({ content: "❌ Faction category not found." });
    }

    try {
        let channelType;
        if (type === 'text') channelType = ChannelType.GuildText;
        else if (type === 'voice') channelType = ChannelType.GuildVoice;
        else return interaction.editReply({ content: "❌ Invalid channel type." });

        const [factionRole, ownerRole, directorRole, hiCommRole, midcommRole] = await Promise.all([
            guild.roles.fetch(faction.role_id),
            guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
            guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
            guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
            guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID)
        ]);

        if (!factionRole) {
            return interaction.editReply({ content: "❌ Faction role not found." });
        }

        const permissionOverwrites = [
            {
                id: guild.id,
                deny: [PermissionFlagsBits.ViewChannel]
            }
        ];

        switch (permissionType) {
            case 'owner_director_only':
                if (ownerRole) {
                    permissionOverwrites.push({
                        id: ownerRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (directorRole) {
                    permissionOverwrites.push({
                        id: directorRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                permissionOverwrites.push({
                    id: factionRole.id,
                    deny: [PermissionFlagsBits.ViewChannel]
                });
                break;
                
            case 'hicomm_readwrite':
                if (ownerRole) {
                    permissionOverwrites.push({
                        id: ownerRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (directorRole) {
                    permissionOverwrites.push({
                        id: directorRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (hiCommRole) {
                    permissionOverwrites.push({
                        id: hiCommRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                permissionOverwrites.push({
                    id: factionRole.id,
                    deny: [PermissionFlagsBits.ViewChannel]
                });
                break;
                
            case 'midcomm_readwrite':
                if (ownerRole) {
                    permissionOverwrites.push({
                        id: ownerRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (directorRole) {
                    permissionOverwrites.push({
                        id: directorRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (hiCommRole) {
                    permissionOverwrites.push({
                        id: hiCommRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                if (midcommRole) {
                    permissionOverwrites.push({
                        id: midcommRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                    });
                }
                permissionOverwrites.push({
                    id: factionRole.id,
                    deny: [PermissionFlagsBits.ViewChannel]
                });
                break;
                
            case 'all_readwrite':
                permissionOverwrites.push({
                    id: factionRole.id,
                    allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory]
                });
                break;
                
            case 'announcements':
                permissionOverwrites.push({
                    id: factionRole.id,
                    allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.ReadMessageHistory],
                    deny: [PermissionFlagsBits.SendMessages]
                });
                if (hiCommRole) {
                    permissionOverwrites.push({
                        id: hiCommRole.id,
                        allow: [PermissionFlagsBits.SendMessages]
                    });
                }
                if (directorRole) {
                    permissionOverwrites.push({
                        id: directorRole.id,
                        allow: [PermissionFlagsBits.SendMessages]
                    });
                }
                if (ownerRole) {
                    permissionOverwrites.push({
                        id: ownerRole.id,
                        allow: [PermissionFlagsBits.SendMessages]
                    });
                }
                break;
                
            case 'voice_all':
                permissionOverwrites.push({
                    id: factionRole.id,
                    allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak]
                });
                break;
                
            case 'voice_midcomm':
                if (ownerRole) {
                    permissionOverwrites.push({
                        id: ownerRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak]
                    });
                }
                if (directorRole) {
                    permissionOverwrites.push({
                        id: directorRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak]
                    });
                }
                if (hiCommRole) {
                    permissionOverwrites.push({
                        id: hiCommRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak]
                    });
                }
                if (midcommRole) {
                    permissionOverwrites.push({
                        id: midcommRole.id,
                        allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak]
                    });
                }
                permissionOverwrites.push({
                    id: factionRole.id,
                    deny: [PermissionFlagsBits.ViewChannel]
                });
                break;
        }

        const channel = await guild.channels.create({
            name: name,
            type: channelType,
            parent: category.id,
            permissionOverwrites: permissionOverwrites
        });

        db.run("INSERT INTO faction_channels (faction_id, channel_type, channel_id, channel_name) VALUES (?, ?, ?, ?)",
            [factionId, key, channel.id, name], async (err) => {
                if (err) {
                    console.error(err);
                    await channel.delete();
                    return interaction.editReply({ content: "❌ Database error while saving channel." });
                }

                const successEmbed = new EmbedBuilder()
                    .setColor(0x00FF00)
                    .setTitle('✅ Channel Created')
                    .setDescription(`Successfully created **${type}** channel: **${name}**`)
                    .addFields(
                        { name: 'Channel Type', value: type, inline: true },
                        { name: 'Channel Key', value: key, inline: true },
                        { name: 'Permission Setting', value: permissionType.replace(/_/g, ' '), inline: true },
                        { name: 'Channel Link', value: `<#${channel.id}>`, inline: false }
                    );

                await interaction.editReply({ embeds: [successEmbed] });
                await logAction('CHANNEL_CREATED', 
                    `${interaction.user.tag} created ${type} channel "${name}" in **${faction.name}** with permissions: ${permissionType}`, 
                    0x00FF00);
            });

    } catch (error) {
        console.error('Error creating channel:', error);
        await interaction.editReply({ content: "❌ Error creating channel." });
    }
}

async function handleFactionCreation(interaction) {
    const name = interaction.fields.getTextInputValue('faction_name');
    const uniqueId = interaction.fields.getTextInputValue('faction_id').toUpperCase();
    const type = interaction.fields.getTextInputValue('faction_type');
    const robloxGroup = interaction.fields.getTextInputValue('roblox_group');
    const color = interaction.fields.getTextInputValue('faction_color');

    if (!color.match(/^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$/)) {
        return interaction.reply({ content: "Invalid color format! Use hex color (e.g., #FF0000).", flags: 64 });
    }

    db.get("SELECT * FROM factions WHERE unique_id = ?", [uniqueId], async (err, row) => {
        if (err) return interaction.reply({ content: "Database error!", flags: 64 });
        if (row) return interaction.reply({ content: "Faction ID already taken!", flags: 64 });

        const factionData = { name, uniqueId, type, robloxGroup, color, ownerId: interaction.user.id, ownerTag: interaction.user.tag };
        pendingFactions.set(uniqueId, factionData);

        const adminChannel = await client.channels.fetch(process.env.ADMIN_CHANNEL_ID);
        const embed = new EmbedBuilder()
            .setColor(0xFFA500)
            .setTitle('New Faction Creation Request')
            .setDescription(`Requested by: \`${factionData.ownerTag}\``)
            .addFields(
                { name: 'Name / ID', value: `${name} / \`${uniqueId}\``, inline: true },
                { name: 'Type', value: type, inline: true },
                { name: 'Color', value: color, inline: true },
                { name: 'Roblox Group', value: `[Link](${robloxGroup})`, inline: true }
            );

        const buttons = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`approve_${uniqueId}`).setLabel('Approve').setStyle(ButtonStyle.Success),
            new ButtonBuilder().setCustomId(`deny_${uniqueId}`).setLabel('Deny').setStyle(ButtonStyle.Danger)
        );
        
        await adminChannel.send({ embeds: [embed], components: [buttons] });
        await interaction.reply({ content: "Faction creation request sent for approval!", flags: 64 });
    });
}

async function handleFactionApproval(interaction, factionId) {
    await interaction.deferUpdate();
    const factionData = pendingFactions.get(factionId);
    if (!factionData) return interaction.editReply({ content: "Faction data not found!", components: [], embeds: [] });

    const guild = interaction.guild;
    const owner = await guild.members.fetch(factionData.ownerId).catch(() => null);
    if (!owner) return interaction.editReply({ content: "Owner not found!", components: [], embeds: [] });

    try {
        const [midcommRole, directorRole, hiCommRole, ownerRole] = await Promise.all([
            guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
            guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
            guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
            guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
        ]);

        const factionRole = await guild.roles.create({
            name: `[${factionData.uniqueId}]`,
            color: factionData.color,
            reason: `Faction creation for ${factionData.uniqueId}`
        });

        const category = await guild.channels.create({
            name: `〡${factionData.uniqueId}`,
            type: ChannelType.GuildCategory
        });

        const categoryPermissionOverwrites = [
            { id: guild.id, deny: [PermissionFlagsBits.ViewChannel] }, 
            { id: factionRole.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.ReadMessageHistory] },
            { id: midcommRole.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] },
            { id: directorRole.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.ManageMessages] },
            { id: hiCommRole.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.ManageMessages] }, 
            { id: ownerRole.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.ManageMessages, PermissionFlagsBits.Administrator] }
        ];
        await category.permissionOverwrites.set(categoryPermissionOverwrites);

        let factionDbId;
        await new Promise((resolve, reject) => {
            db.run(`INSERT INTO factions (name, unique_id, type, roblox_group, color, owner_id, role_id, hicomm_role_id, director_role_id, midcomm_role_id, category_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, 
                [factionData.name, factionData.uniqueId, factionData.type, factionData.robloxGroup, factionData.color, factionData.ownerId, 
                 factionRole.id, hiCommRole.id, directorRole.id, midcommRole.id, category.id],
                function(err) { 
                if (err) return reject(err);
                factionDbId = this.lastID;
                resolve();
            });
        });

        await updateFactionPermissions(factionDbId, DEFAULT_PERMISSIONS);

        db.run(`INSERT INTO faction_members (faction_id, user_id, role) VALUES (?, ?, ?)`, [factionDbId, factionData.ownerId, 'owner']);

        for (const channelDef of channelTypes) {
            let overrides = category.permissionOverwrites.cache.map(ov => ({ 
                id: ov.id, allow: ov.allow.bitfield, deny: ov.deny.bitfield 
            }));

            if (channelDef.permissions === 'readonly') {
                const denySend = { deny: [PermissionFlagsBits.SendMessages] };
                overrides.push({ id: factionRole.id, ...denySend });
                overrides.push({ id: midcommRole.id, ...denySend }); 
            } else if (channelDef.permissions === 'readwrite') {
                overrides.push({ id: factionRole.id, allow: [PermissionFlagsBits.SendMessages] });
            } else if (channelDef.permissions === 'voice') {
                const voiceAllow = [PermissionFlagsBits.Connect, PermissionFlagsBits.Speak];
                overrides.push({ id: factionRole.id, allow: voiceAllow });
                overrides.push({ id: midcommRole.id, allow: voiceAllow }); 
            }

            const channel = await guild.channels.create({
                name: channelDef.name, 
                type: channelDef.type,
                parent: category.id,
                permissionOverwrites: overrides
            });

            db.run("INSERT INTO faction_channels (faction_id, channel_type, channel_id, channel_name) VALUES (?, ?, ?, ?)", 
                [factionDbId, channelDef.key, channel.id, channel.name]);
        }

        await owner.roles.add([factionRole.id, ownerRole.id]);
        await safeSetNickname(owner, `[${factionData.uniqueId}] ${owner.user.username}`);
        await owner.send(`Your faction **${factionData.name}** has been approved!`).catch(() => {});

        pendingFactions.delete(factionId);
        await interaction.editReply({ content: `Faction **${factionData.name}** approved!`, components: [], embeds: [] });
        await logAction('FACTION_CREATED', `Faction **${factionData.name}** created for ${owner.user.tag}`, 0x00FF00);

    } catch (error) {
        console.error('Faction Approval Error:', error);
        await interaction.editReply({ content: "Error during faction creation.", components: [], embeds: [] });
    }
}

async function handlePromoteMember(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'promote')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'promote');
    
    const members = await getFactionMembers(faction.id);
    const promoterLevel = getRoleLevelIndex(faction.member_role);
    
    const promotableMembers = members.filter(member => {
        const targetLevel = getRoleLevelIndex(member.role);
        return member.user_id !== interaction.user.id && 
               targetLevel < promoterLevel && 
               PROMOTABLE_RANKS.includes(member.role.toLowerCase());
    });

    if (promotableMembers.length === 0) {
        return interaction.reply({ content: "❌ No members available to promote.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`promote_member_${faction.id}`)
        .setPlaceholder('Select member to promote...')
        .addOptions(promotableMembers.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`Promote this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0x00FF00)
        .setTitle('⬆️ Promote Member')
        .setDescription('Select a member to promote:')
        .setFooter({ text: 'You can only promote members lower than your rank' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handlePromoteMemberSelect(interaction) {
    const factionId = interaction.customId.replace('promote_member_', '');
    const targetId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!targetMember) {
        return interaction.reply({ content: "❌ Member not found.", flags: 64 });
    }

    const currentRankIndex = RANK_ORDER.indexOf(targetMember.role.toLowerCase());
    const availableRanks = RANK_ORDER.slice(0, currentRankIndex).filter(rank => PROMOTABLE_RANKS.includes(rank));

    if (availableRanks.length === 0) {
        return interaction.reply({ content: "❌ This member cannot be promoted further.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`promote_rank_${factionId}_${targetId}`)
        .setPlaceholder('Select new rank...')
        .addOptions(availableRanks.map(rank => 
            new StringSelectMenuOptionBuilder()
                .setLabel(rank.toUpperCase())
                .setValue(rank)
                .setDescription(`Promote to ${rank}`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0x00FF00)
        .setTitle('⬆️ Select New Rank')
        .setDescription(`Promoting <@${targetId}> from ${targetMember.role.toUpperCase()}`)
        .setFooter({ text: 'Select the new rank for this member' });

    await interaction.update({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)]
    });
}

async function handlePromoteRankSelect(interaction) {
    const [factionId, targetId] = interaction.customId.replace('promote_rank_', '').split('_');
    const newRank = interaction.values[0];
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!faction || !targetMember) {
        return interaction.editReply({ content: "❌ Member not found." });
    }

    try {
        const guild = interaction.guild;
        
        db.run("UPDATE faction_members SET role = ? WHERE faction_id = ? AND user_id = ?", 
            [newRank, factionId, targetId], async (err) => {
                if (err) {
                    console.error(err);
                    return interaction.editReply({ content: "❌ Database error while promoting member." });
                }

                const member = await guild.members.fetch(targetId).catch(() => null);
                if (member) {
                    const [ownerRole, directorRole, hiCommRole, midcommRole, memberRole] = await Promise.all([
                        guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                        guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
                        guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                        guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                        guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID)
                    ]);

                    const rolesToRemove = [ownerRole, directorRole, hiCommRole, midcommRole, memberRole].filter(role => role && member.roles.cache.has(role.id));
                    if (rolesToRemove.length > 0) {
                        await member.roles.remove(rolesToRemove);
                    }

                    let newGlobalRole = null;
                    if (newRank === 'owner' && ownerRole) newGlobalRole = ownerRole;
                    else if (newRank === 'director' && directorRole) newGlobalRole = directorRole;
                    else if (newRank === 'hicomm' && hiCommRole) newGlobalRole = hiCommRole;
                    else if (newRank === 'midcomm' && midcommRole) newGlobalRole = midcommRole;
                    else if (newRank === 'member' && memberRole) newGlobalRole = memberRole;

                    if (newGlobalRole) {
                        await member.roles.add(newGlobalRole);
                    }
                }

                const successEmbed = new EmbedBuilder()
                    .setColor(0x00FF00)
                    .setTitle('✅ Member Promoted')
                    .setDescription(`<@${targetId}> has been promoted!`)
                    .addFields(
                        { name: 'Old Rank', value: targetMember.role.toUpperCase(), inline: true },
                        { name: 'New Rank', value: newRank.toUpperCase(), inline: true },
                        { name: 'Promoted By', value: `<@${interaction.user.id}>`, inline: true }
                    )
                    .setFooter({ text: 'Rank updated successfully' });

                await interaction.editReply({ embeds: [successEmbed], components: [] });
                await logAction('MEMBER_PROMOTED', 
                    `<@${interaction.user.id}> promoted <@${targetId}> from ${targetMember.role.toUpperCase()} to ${newRank.toUpperCase()} in **${faction.name}**`, 
                    0x00FF00);
            });

    } catch (error) {
        console.error('Error promoting member:', error);
        await interaction.editReply({ content: "❌ Error promoting member." });
    }
}

async function handleDemoteMember(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'demote')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'demote');
    
    const members = await getFactionMembers(faction.id);
    const demoterLevel = getRoleLevelIndex(faction.member_role);
    
    const demotableMembers = members.filter(member => {
        const targetLevel = getRoleLevelIndex(member.role);
        return member.user_id !== interaction.user.id && 
               targetLevel < demoterLevel && 
               targetLevel > 0;
    });

    if (demotableMembers.length === 0) {
        return interaction.reply({ content: "❌ No members available to demote.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`demote_member_${faction.id}`)
        .setPlaceholder('Select member to demote...')
        .addOptions(demotableMembers.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`Demote this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('⬇️ Demote Member')
        .setDescription('Select a member to demote:')
        .setFooter({ text: 'You can only demote members lower than your rank' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleDemoteMemberSelect(interaction) {
    const factionId = interaction.customId.replace('demote_member_', '');
    const targetId = interaction.values[0];
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!targetMember) {
        return interaction.reply({ content: "❌ Member not found.", flags: 64 });
    }

    const currentRankIndex = RANK_ORDER.indexOf(targetMember.role.toLowerCase());
    const availableRanks = RANK_ORDER.slice(currentRankIndex + 1).filter(rank => rank !== 'owner');

    if (availableRanks.length === 0) {
        return interaction.reply({ content: "❌ This member cannot be demoted further.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`demote_rank_${factionId}_${targetId}`)
        .setPlaceholder('Select new rank...')
        .addOptions(availableRanks.map(rank => 
            new StringSelectMenuOptionBuilder()
                .setLabel(rank.toUpperCase())
                .setValue(rank)
                .setDescription(`Demote to ${rank}`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('⬇️ Select New Rank')
        .setDescription(`Demoting <@${targetId}> from ${targetMember.role.toUpperCase()}`)
        .setFooter({ text: 'Select the new rank for this member' });

    await interaction.update({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)]
    });
}

async function handleDemoteRankSelect(interaction) {
    const [factionId, targetId] = interaction.customId.replace('demote_rank_', '').split('_');
    const newRank = interaction.values[0];
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!faction || !targetMember) {
        return interaction.editReply({ content: "❌ Member not found." });
    }

    try {
        const guild = interaction.guild;
        
        db.run("UPDATE faction_members SET role = ? WHERE faction_id = ? AND user_id = ?", 
            [newRank, factionId, targetId], async (err) => {
                if (err) {
                    console.error(err);
                    return interaction.editReply({ content: "❌ Database error while demoting member." });
                }

                const member = await guild.members.fetch(targetId).catch(() => null);
                if (member) {
                    const [ownerRole, directorRole, hiCommRole, midcommRole, memberRole] = await Promise.all([
                        guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                        guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
                        guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                        guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                        guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID)
                    ]);

                    const rolesToRemove = [ownerRole, directorRole, hiCommRole, midcommRole, memberRole].filter(role => role && member.roles.cache.has(role.id));
                    if (rolesToRemove.length > 0) {
                        await member.roles.remove(rolesToRemove);
                    }

                    let newGlobalRole = null;
                    if (newRank === 'owner' && ownerRole) newGlobalRole = ownerRole;
                    else if (newRank === 'director' && directorRole) newGlobalRole = directorRole;
                    else if (newRank === 'hicomm' && hiCommRole) newGlobalRole = hiCommRole;
                    else if (newRank === 'midcomm' && midcommRole) newGlobalRole = midcommRole;
                    else if (newRank === 'member' && memberRole) newGlobalRole = memberRole;

                    if (newGlobalRole) {
                        await member.roles.add(newGlobalRole);
                    }
                }

                const successEmbed = new EmbedBuilder()
                    .setColor(0xFFA500)
                    .setTitle('✅ Member Demoted')
                    .setDescription(`<@${targetId}> has been demoted!`)
                    .addFields(
                        { name: 'Old Rank', value: targetMember.role.toUpperCase(), inline: true },
                        { name: 'New Rank', value: newRank.toUpperCase(), inline: true },
                        { name: 'Demoted By', value: `<@${interaction.user.id}>`, inline: true }
                    )
                    .setFooter({ text: 'Rank updated successfully' });

                await interaction.editReply({ embeds: [successEmbed], components: [] });
                await logAction('MEMBER_DEMOTED', 
                    `<@${interaction.user.id}> demoted <@${targetId}> from ${targetMember.role.toUpperCase()} to ${newRank.toUpperCase()} in **${faction.name}**`, 
                    0xFFA500);
            });

    } catch (error) {
        console.error('Error demoting member:', error);
        await interaction.editReply({ content: "❌ Error demoting member." });
    }
}

async function handleKickMember(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'kick')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'kick');
    
    const members = await getFactionMembers(faction.id);
    const kickerLevel = getRoleLevelIndex(faction.member_role);
    
    const kickableMembers = members.filter(member => {
        const targetLevel = getRoleLevelIndex(member.role);
        return member.user_id !== interaction.user.id && targetLevel < kickerLevel;
    });

    if (kickableMembers.length === 0) {
        return interaction.reply({ content: "❌ No members available to kick.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`kick_${faction.id}`)
        .setPlaceholder('Select member to kick...')
        .addOptions(kickableMembers.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`Kick this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0xFF0000)
        .setTitle('👢 Kick Member')
        .setDescription('Select a member to kick from the faction:')
        .setFooter({ text: 'This action cannot be undone!' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleKickSelect(interaction) {
    const factionId = interaction.customId.replace('kick_', '');
    const targetId = interaction.values[0];
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    const targetMember = await getFactionMember(factionId, targetId);
    
    if (!faction || !targetMember) {
        return interaction.editReply({ content: "❌ Member not found." });
    }

    try {
        const guild = interaction.guild;
        
        db.run("DELETE FROM faction_members WHERE faction_id = ? AND user_id = ?", [factionId, targetId], async (err) => {
            if (err) {
                console.error(err);
                return interaction.editReply({ content: "❌ Database error while kicking member." });
            }

            const member = await guild.members.fetch(targetId).catch(() => null);
            if (member) {
                const factionRole = await guild.roles.fetch(faction.role_id);
                if (factionRole && member.roles.cache.has(factionRole.id)) {
                    await member.roles.remove(factionRole.id);
                }

                const [ownerRole, directorRole, hiCommRole, midcommRole, memberRole] = await Promise.all([
                    guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                    guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
                    guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                    guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                    guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID)
                ]);

                const rolesToRemove = [ownerRole, directorRole, hiCommRole, midcommRole, memberRole].filter(role => role && member.roles.cache.has(role.id));
                if (rolesToRemove.length > 0) {
                    await member.roles.remove(rolesToRemove);
                }

                await safeSetNickname(member, null);
            }

            await clearAllStrikes(factionId, targetId);

            const successEmbed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('✅ Member Kicked')
                .setDescription(`<@${targetId}> has been kicked from the faction!`)
                .addFields(
                    { name: 'Former Rank', value: targetMember.role.toUpperCase(), inline: true },
                    { name: 'Kicked By', value: `<@${interaction.user.id}>`, inline: true }
                )
                .setFooter({ text: 'Member removed from faction' });

            await interaction.editReply({ embeds: [successEmbed], components: [] });
            await logAction('MEMBER_KICKED', 
                `<@${interaction.user.id}> kicked <@${targetId}> (${targetMember.role.toUpperCase()}) from **${faction.name}**`, 
                0xFF0000);
        });

    } catch (error) {
        console.error('Error kicking member:', error);
        await interaction.editReply({ content: "❌ Error kicking member." });
    }
}

async function handleTransferOwnership(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'transfer')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'transfer');
    
    const members = await getFactionMembers(faction.id);
    
    const transferableMembers = members.filter(member => 
        member.user_id !== interaction.user.id && member.role.toLowerCase() !== 'owner'
    );

    if (transferableMembers.length === 0) {
        return interaction.reply({ content: "❌ No members available to transfer ownership to.", flags: 64 });
    }

    const selectMenu = new StringSelectMenuBuilder()
        .setCustomId(`transfer_${faction.id}`)
        .setPlaceholder('Select new owner...')
        .addOptions(transferableMembers.map(member => 
            new StringSelectMenuOptionBuilder()
                .setLabel(`${member.role.toUpperCase()}: ${member.user_id}`)
                .setValue(member.user_id)
                .setDescription(`Transfer ownership to this member`)
        ));

    const embed = new EmbedBuilder()
        .setColor(0x9B59B6)
        .setTitle('👑 Transfer Ownership')
        .setDescription('Select a member to transfer faction ownership to:')
        .setFooter({ text: 'This action is irreversible!' });

    await interaction.reply({ 
        embeds: [embed], 
        components: [new ActionRowBuilder().addComponents(selectMenu)],
        flags: 64 
    });
}

async function handleTransferSelect(interaction) {
    const factionId = interaction.customId.replace('transfer_', '');
    const newOwnerId = interaction.values[0];
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    const newOwnerMember = await getFactionMember(factionId, newOwnerId);
    
    if (!faction || !newOwnerMember) {
        return interaction.editReply({ content: "❌ Member not found." });
    }

    try {
        const guild = interaction.guild;
        
        db.serialize(() => {
            db.run("UPDATE faction_members SET role = 'hicomm' WHERE faction_id = ? AND user_id = ?", [factionId, interaction.user.id]);
            db.run("UPDATE faction_members SET role = 'owner' WHERE faction_id = ? AND user_id = ?", [factionId, newOwnerId]);
            db.run("UPDATE factions SET owner_id = ? WHERE id = ?", [newOwnerId, factionId]);
        });

        const [oldOwner, newOwner] = await Promise.all([
            guild.members.fetch(interaction.user.id),
            guild.members.fetch(newOwnerId)
        ]);

        if (oldOwner && newOwner) {
            const [ownerRole, hicommRole] = await Promise.all([
                guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID)
            ]);

            if (ownerRole && hicommRole) {
                if (oldOwner.roles.cache.has(ownerRole.id)) {
                    await oldOwner.roles.remove(ownerRole.id);
                }
                await oldOwner.roles.add(hicommRole.id);

                if (newOwner.roles.cache.has(hicommRole.id)) {
                    await newOwner.roles.remove(hicommRole.id);
                }
                await newOwner.roles.add(ownerRole.id);
            }
        }

        const successEmbed = new EmbedBuilder()
            .setColor(0x9B59B6)
            .setTitle('✅ Ownership Transferred')
            .setDescription(`Faction ownership has been transferred!`)
            .addFields(
                { name: 'New Owner', value: `<@${newOwnerId}>`, inline: true },
                { name: 'Previous Owner', value: `<@${interaction.user.id}>`, inline: true },
                { name: 'Faction', value: faction.name, inline: true }
            )
            .setFooter({ text: 'Ownership transfer complete' });

        await interaction.editReply({ embeds: [successEmbed], components: [] });
        await logAction('OWNERSHIP_TRANSFERRED', 
            `<@${interaction.user.id}> transferred ownership of **${faction.name}** to <@${newOwnerId}>`, 
            0x9B59B6);

    } catch (error) {
        console.error('Error transferring ownership:', error);
        await interaction.editReply({ content: "❌ Error transferring ownership." });
    }
}

async function handleLeaveFaction(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'leave')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'leave');
    
    if (faction.owner_id === interaction.user.id) {
        return interaction.reply({ content: "❌ You cannot leave your own faction. Transfer ownership first or disband the faction.", flags: 64 });
    }

    const confirmEmbed = new EmbedBuilder()
        .setColor(0xFFA500)
        .setTitle('⚠️ Leave Faction')
        .setDescription(`Are you sure you want to leave **${faction.name}**?`)
        .addFields(
            { name: 'Faction', value: faction.name, inline: true },
            { name: 'Your Rank', value: faction.member_role.toUpperCase(), inline: true }
        )
        .setFooter({ text: 'This action cannot be undone!' });

    const buttons = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`confirm_leave_${faction.id}`)
            .setLabel('Yes, Leave')
            .setStyle(ButtonStyle.Danger),
        new ButtonBuilder()
            .setCustomId('cancel_leave')
            .setLabel('Cancel')
            .setStyle(ButtonStyle.Secondary)
    );

    await interaction.reply({ embeds: [confirmEmbed], components: [buttons], flags: 64 });
}

async function handleConfirmLeave(interaction) {
    const factionId = interaction.customId.replace('confirm_leave_', '');
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    const member = await getFactionMember(factionId, interaction.user.id);
    
    if (!faction || !member) {
        return interaction.editReply({ content: "❌ Faction or member not found." });
    }

    try {
        const guild = interaction.guild;
        
        db.run("DELETE FROM faction_members WHERE faction_id = ? AND user_id = ?", [factionId, interaction.user.id], async (err) => {
            if (err) {
                console.error(err);
                return interaction.editReply({ content: "❌ Database error while leaving faction." });
            }

            const userMember = await guild.members.fetch(interaction.user.id).catch(() => null);
            if (userMember) {
                const factionRole = await guild.roles.fetch(faction.role_id);
                if (factionRole && userMember.roles.cache.has(factionRole.id)) {
                    await userMember.roles.remove(factionRole.id);
                }

                const [ownerRole, directorRole, hiCommRole, midcommRole, memberRole] = await Promise.all([
                    guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                    guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID),
                    guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                    guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                    guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID)
                ]);

                const rolesToRemove = [ownerRole, directorRole, hiCommRole, midcommRole, memberRole].filter(role => role && userMember.roles.cache.has(role.id));
                if (rolesToRemove.length > 0) {
                    await userMember.roles.remove(rolesToRemove);
                }

                await safeSetNickname(userMember, null);
            }

            await clearAllStrikes(factionId, interaction.user.id);

            const successEmbed = new EmbedBuilder()
                .setColor(0xFFA500)
                .setTitle('✅ Left Faction')
                .setDescription(`You have left **${faction.name}**`)
                .addFields(
                    { name: 'Former Rank', value: member.role.toUpperCase(), inline: true },
                    { name: 'Faction', value: faction.name, inline: true }
                )
                .setFooter({ text: 'You are no longer a member of this faction' });

            await interaction.editReply({ embeds: [successEmbed], components: [] });
            await logAction('MEMBER_LEFT', 
                `<@${interaction.user.id}> (${member.role.toUpperCase()}) left **${faction.name}**`, 
                0xFFA500);
        });

    } catch (error) {
        console.error('Error leaving faction:', error);
        await interaction.editReply({ content: "❌ Error leaving faction." });
    }
}

async function handleConfirmDisband(interaction) {
    const factionId = interaction.customId.replace('confirm_disband_', '');
    
    await interaction.deferUpdate();
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "❌ Faction not found.", components: [] });
    }

    try {
        const guild = interaction.guild;
        
        const factionMembers = await getFactionMembers(factionId);
        
        try {
            const category = await guild.channels.fetch(faction.category_id);
            if (category) {
                const channels = await getFactionChannels(factionId);
                for (const channelData of channels) {
                    try {
                        const channel = await guild.channels.fetch(channelData.channel_id);
                        if (channel) await channel.delete();
                    } catch (error) {}
                }
                await category.delete();
            }
            
            const [factionRole, ownerRole, memberRole, midcommRole, hiCommRole, directorRole] = await Promise.all([
                guild.roles.fetch(faction.role_id),
                guild.roles.fetch(GLOBAL_OWNER_ROLE_ID),
                guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID),
                guild.roles.fetch(GLOBAL_MIDCOMM_ROLE_ID),
                guild.roles.fetch(GLOBAL_HICOMM_ROLE_ID),
                guild.roles.fetch(GLOBAL_DIRECTOR_ROLE_ID)
            ]);
            
            for (const memberData of factionMembers) {
                try {
                    const member = await guild.members.fetch(memberData.user_id);
                    
                    if (factionRole && member.roles.cache.has(factionRole.id)) {
                        await member.roles.remove(factionRole.id);
                    }
                    
                    if (memberData.role.toLowerCase() === 'owner' && ownerRole && member.roles.cache.has(ownerRole.id)) {
                        await member.roles.remove(ownerRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'director' && directorRole && member.roles.cache.has(directorRole.id)) {
                        await member.roles.remove(directorRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'hicomm' && hiCommRole && member.roles.cache.has(hiCommRole.id)) {
                        await member.roles.remove(hiCommRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'midcomm' && midcommRole && member.roles.cache.has(midcommRole.id)) {
                        await member.roles.remove(midcommRole.id);
                    }
                    if (memberData.role.toLowerCase() === 'member' && memberRole && member.roles.cache.has(memberRole.id)) {
                        await member.roles.remove(memberRole.id);
                    }
                    
                    await safeSetNickname(member, null);
                    
                } catch (error) {}
            }
            
            if (factionRole) await factionRole.delete();
            
        } catch (discordError) {
            console.error('Error deleting Discord resources:', discordError);
        }
        
        await new Promise((resolve, reject) => {
            db.serialize(() => {
                db.run("DELETE FROM faction_strikes WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_members WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_channels WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM faction_permissions WHERE faction_id = ?", [factionId]);
                db.run("DELETE FROM factions WHERE id = ?", [factionId], function(err) {
                    if (err) reject(err);
                    else resolve();
                });
            });
        });
        
        const successEmbed = new EmbedBuilder()
            .setColor(0xFF0000)
            .setTitle('✅ Faction Disbanded')
            .setDescription(`**${faction.name}** has been disbanded!`)
            .addFields(
                { name: 'Former Owner', value: `<@${faction.owner_id}>`, inline: true },
                { name: 'Disbanded By', value: `<@${interaction.user.id}>`, inline: true },
                { name: 'Members Affected', value: `${factionMembers.length} members`, inline: true }
            )
            .setFooter({ text: 'All faction data has been permanently deleted' });

        await interaction.editReply({ embeds: [successEmbed], components: [] });
        await logAction('FACTION_DISBANDED', 
            `<@${interaction.user.id}> disbanded **${faction.name}** (Owner: <@${faction.owner_id}>, Members: ${factionMembers.length})`, 
            0xFF0000);
            
    } catch (error) {
        console.error('Error disbanding faction:', error);
        await interaction.editReply({ content: "❌ Error disbanding faction.", components: [] });
    }
}

async function handleInviteMembers(interaction, faction) {
    if (isOnCommandCooldown(interaction.user.id, 'invite')) {
        return interaction.reply({ content: "⏰ Please wait a few seconds before using this command again.", flags: 64 });
    }
    setCommandCooldown(interaction.user.id, 'invite');
    
    const modal = new ModalBuilder()
        .setCustomId(`invite_${faction.id}`)
        .setTitle('Invite Members');

    const userIdsInput = new TextInputBuilder()
        .setCustomId('user_ids')
        .setLabel('User IDs (separate with commas)')
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setPlaceholder('123456789012345678, 987654321098765432')
        .setMaxLength(1000);

    modal.addComponents(new ActionRowBuilder().addComponents(userIdsInput));
    await interaction.showModal(modal);
}

async function handleInviteModal(interaction, factionId) {
    const userIdsText = interaction.fields.getTextInputValue('user_ids');
    const userIds = userIdsText.split(',').map(id => id.trim()).filter(id => id.length > 0);
    
    await interaction.deferReply({ flags: 64 });
    
    const faction = await getFactionById(factionId);
    if (!faction) {
        return interaction.editReply({ content: "❌ Faction not found." });
    }

    if (userIds.length === 0) {
        return interaction.editReply({ content: "❌ No valid user IDs provided." });
    }

    if (userIds.length > 10) {
        return interaction.editReply({ content: "❌ You can only invite up to 10 members at once." });
    }

    const guild = interaction.guild;
    const results = {
        success: [],
        failed: [],
        alreadyInFaction: [],
        invalid: []
    };

    for (const userId of userIds) {
        try {
            if (!/^\d+$/.test(userId)) {
                results.invalid.push(userId);
                continue;
            }

            const existingMember = await getFactionMember(factionId, userId);
            if (existingMember) {
                results.alreadyInFaction.push(userId);
                continue;
            }

            const member = await guild.members.fetch(userId).catch(() => null);
            if (!member) {
                results.failed.push(userId);
                continue;
            }

            const existingFaction = await getFactionByMember(userId);
            if (existingFaction) {
                results.alreadyInFaction.push(userId);
                continue;
            }

            db.run("INSERT INTO faction_members (faction_id, user_id, role) VALUES (?, ?, 'member')", 
                [factionId, userId], async (err) => {
                    if (err) {
                        results.failed.push(userId);
                    } else {
                        results.success.push(userId);
                        
                        const factionRole = await guild.roles.fetch(faction.role_id);
                        const memberRole = await guild.roles.fetch(GLOBAL_MEMBER_ROLE_ID);
                        
                        if (factionRole) await member.roles.add(factionRole.id);
                        if (memberRole) await member.roles.add(memberRole.id);
                        await safeSetNickname(member, `[${faction.unique_id}] ${member.user.username}`);
                        
                        try {
                            await member.send(`You have been invited to join **${faction.name}**!`);
                        } catch (dmError) {}
                    }
                });
                
        } catch (error) {
            results.failed.push(userId);
        }
    }

    setTimeout(async () => {
        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('📩 Invite Results')
            .setDescription(`Invitation results for **${faction.name}**`)
            .setFooter({ text: `Processed ${userIds.length} users` });

        if (results.success.length > 0) {
            embed.addFields({
                name: '✅ Successfully Invited',
                value: results.success.map(id => `<@${id}>`).join(', ') || 'None',
                inline: false
            });
        }

        if (results.alreadyInFaction.length > 0) {
            embed.addFields({
                name: '⚠️ Already in a Faction',
                value: results.alreadyInFaction.map(id => `<@${id}>`).join(', ') || 'None',
                inline: false
            });
        }

        if (results.failed.length > 0) {
            embed.addFields({
                name: '❌ Failed to Invite',
                value: results.failed.map(id => `<@${id}>`).join(', ') || 'None',
                inline: false
            });
        }

        if (results.invalid.length > 0) {
            embed.addFields({
                name: '❌ Invalid User IDs',
                value: results.invalid.join(', ') || 'None',
                inline: false
            });
        }

        await interaction.editReply({ embeds: [embed] });
        await logAction('MEMBERS_INVITED', 
            `<@${interaction.user.id}> invited ${results.success.length} members to **${faction.name}**`, 
            0x00FF00);
    }, 1000);
}

client.on('interactionCreate', async interaction => {
    try {
        if (interaction.isChatInputCommand()) {
            const { commandName } = interaction;
            switch (commandName) {
                case 'create': 
                    await handleCreate(interaction); 
                    break;
                case 'manage': 
                    await handleManage(interaction); 
                    break;
                case 'admin':
                    await handleAdmin(interaction);
                    break;
                case 'list':
                    await handleList(interaction);
                    break;
            }
        }

        if (interaction.isButton()) {
            const customId = interaction.customId;
            if (customId.startsWith('approve_')) {
                await handleFactionApproval(interaction, customId.replace('approve_', ''));
            } else if (customId.startsWith('deny_')) {
                await interaction.update({ content: `Faction creation denied.`, components: [], embeds: [] });
            } else if (customId.startsWith('confirm_leave_')) {
                await handleConfirmLeave(interaction);
            } else if (customId === 'cancel_leave') {
                await interaction.update({ content: "Leave cancelled.", components: [], embeds: [] });
            } else if (customId.startsWith('confirm_disband_')) {
                await handleConfirmDisband(interaction);
            } else if (customId === 'cancel_disband') {
                await interaction.update({ content: "Disband cancelled.", components: [], embeds: [] });
            } else if (customId.startsWith('admin_confirm_disband_')) {
                await handleAdminConfirmDisband(interaction);
            } else if (customId === 'admin_cancel_disband') {
                await handleAdminCancelDisband(interaction);
            } else if (customId.startsWith('settings_edit_')) {
                const factionId = customId.replace('settings_edit_', '');
                await handleSettingsEdit(interaction, factionId);
            } else if (customId.startsWith('settings_reset_')) {
                const factionId = customId.replace('settings_reset_', '');
                await handleSettingsReset(interaction, factionId);
            } else if (customId === 'settings_cancel') {
                await handleSettingsCancel(interaction);
            }
        }

        if (interaction.isModalSubmit()) {
            const customId = interaction.customId;
            if (customId === 'create_faction') {
                await handleFactionCreation(interaction);
            } else if (customId.startsWith('invite_')) {
                const factionId = customId.replace('invite_', '');
                await handleInviteModal(interaction, factionId);
            } else if (customId.startsWith('add_channel_')) {
                const factionId = customId.replace('add_channel_', '');
                await handleAddChannelModal(interaction, factionId);
            } else if (customId.startsWith('rename_channel_modal_')) {
                const parts = customId.replace('rename_channel_modal_', '').split('_');
                const factionId = parts[0];
                const channelId = parts[1];
                await handleRenameChannelModal(interaction, factionId, channelId);
            } else if (customId.startsWith('admin_rename_')) {
                await handleAdminRenameModal(interaction);
            } else if (customId.startsWith('strike_reason_')) {
                await handleStrikeReasonModal(interaction);
            }
        }

        if (interaction.isStringSelectMenu()) {
            if (interaction.customId === 'manage_select') {
                const action = interaction.values[0];
                const faction = await getFactionByMember(interaction.user.id);
                if (!faction) return interaction.reply({ content: "Faction not found.", flags: 64 });

                switch (action) {
                    case 'rename':
                        const renameModal = new ModalBuilder().setCustomId(`rename_${faction.id}`).setTitle('Rename Faction');
                        renameModal.addComponents(new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('new_name').setLabel('New Faction Name').setStyle(TextInputStyle.Short).setRequired(true)));
                        await interaction.showModal(renameModal);
                        break;
                    case 'disband':
                        const confirmEmbed = new EmbedBuilder().setColor(0xFF0000).setTitle('Confirm Disbandment').setDescription(`Disband ${faction.name}? This is permanent!`);
                        const buttons = new ActionRowBuilder().addComponents(
                            new ButtonBuilder().setCustomId(`confirm_disband_${faction.id}`).setLabel('Yes, Disband').setStyle(ButtonStyle.Danger),
                            new ButtonBuilder().setCustomId('cancel_disband').setLabel('Cancel').setStyle(ButtonStyle.Secondary)
                        );
                        await interaction.reply({ embeds: [confirmEmbed], components: [buttons], flags: 64 });
                        break;
                    case 'promote':
                        await handlePromoteMember(interaction, faction);
                        break;
                    case 'demote':
                        await handleDemoteMember(interaction, faction);
                        break;
                    case 'invite':
                        await handleInviteMembers(interaction, faction);
                        break;
                    case 'kick':
                        await handleKickMember(interaction, faction);
                        break;
                    case 'strike':
                        await handleStrikeMember(interaction, faction);
                        break;
                    case 'view_strikes':
                        await handleViewStrikes(interaction, faction);
                        break;
                    case 'remove_strike':
                        await handleRemoveStrike(interaction, faction);
                        break;
                    case 'transfer':
                        await handleTransferOwnership(interaction, faction);
                        break;
                    case 'leave':
                        await handleLeaveFaction(interaction, faction);
                        break;
                    case 'settings':
                        await handleFactionSettings(interaction, faction);
                        break;
                    case 'add_channel':
                        await handleAddNewChannel(interaction, faction);
                        break;
                    case 'remove_channel':
                        await handleRemoveChannel(interaction, faction);
                        break;
                    case 'rename_channel':
                        await handleRenameChannel(interaction, faction);
                        break;
                    default:
                        await interaction.reply({ content: `Selected: ${action}`, flags: 64 });
                }
            } else if (interaction.customId.startsWith('promote_member_')) {
                await handlePromoteMemberSelect(interaction);
            } else if (interaction.customId.startsWith('demote_member_')) {
                await handleDemoteMemberSelect(interaction);
            } else if (interaction.customId.startsWith('promote_rank_')) {
                await handlePromoteRankSelect(interaction);
            } else if (interaction.customId.startsWith('demote_rank_')) {
                await handleDemoteRankSelect(interaction);
            } else if (interaction.customId.startsWith('kick_')) {
                await handleKickSelect(interaction);
            } else if (interaction.customId.startsWith('transfer_')) {
                await handleTransferSelect(interaction);
            } else if (interaction.customId.startsWith('strike_select_')) {
                await handleStrikeSelect(interaction);
            } else if (interaction.customId.startsWith('view_strikes_')) {
                await handleViewStrikesSelect(interaction);
            } else if (interaction.customId.startsWith('remove_strike_select_')) {
                await handleRemoveStrikeSelect(interaction);
            } else if (interaction.customId.startsWith('remove_strike_confirm_')) {
                await handleRemoveStrikeConfirm(interaction);
            } else if (interaction.customId.startsWith('remove_channel_')) {
                await handleRemoveChannelSelect(interaction);
            } else if (interaction.customId.startsWith('rename_channel_select_')) {
                await handleRenameChannelSelect(interaction);
            } else if (interaction.customId === 'admin_action_select') {
                await handleAdminActionSelect(interaction);
            } else if (interaction.customId.startsWith('admin_force_rename_faction')) {
                await handleAdminRenameSelect(interaction);
            } else if (interaction.customId.startsWith('admin_force_disband_faction')) {
                await handleAdminDisbandSelect(interaction);
            } else if (interaction.customId.startsWith('channel_type_select_')) {
                await handleChannelTypeSelect(interaction);
            } else if (interaction.customId.startsWith('channel_perms_')) {
                await handleChannelPermissionSelect(interaction);
            } else if (interaction.customId.startsWith('settings_select_action_')) {
                const factionId = interaction.customId.replace('settings_select_action_', '');
                await handleSettingsSelectAction(interaction, factionId);
            } else if (interaction.customId.startsWith('settings_set_level_')) {
                const parts = interaction.customId.replace('settings_set_level_', '').split('_');
                const factionId = parts[0];
                const action = parts[1];
                await handleSettingsSetLevel(interaction, factionId, action);
            }
        }
    } catch (error) {
        console.error('Interaction error:', error);
        try {
            if (interaction.deferred || interaction.replied) {
                await interaction.followUp({ content: 'An error occurred!', flags: 64 });
            } else {
                await interaction.reply({ content: 'An error occurred!', flags: 64 });
            }
        } catch (e) {}
    }
});

client.login(process.env.BOT_TOKEN);


async function fetchJsonFromAttachment(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch file: ${res.status}`);
  return await res.json();
}

async function createRoleFromExport(guild, roleExport, factionName) {
  const roleName = roleExport?.name || `${factionName} | Member`;
  const hexColor = (roleExport?.color || roleExport?.hexColor || null) || null;
  const mentionable = roleExport?.mentionable ?? false;
  const hoist = roleExport?.hoist ?? false;

  const roleData = { name: roleName, mentionable, hoist };
  if (hexColor && hexColor !== '#000000') roleData.color = hexColor;

  try {
    const role = await guild.roles.create(roleData);
    return role;
  } catch (error) {
    console.error('createRoleFromExport error', error);
    return null;
  }
}

async function createCategoryFromExport(guild, categoryExport, factionName) {
  const name = categoryExport?.name || `〡${factionName}`;
  try {
    const category = await guild.channels.create({ name, type: 4 });
    return category;
  } catch (error) {
    console.error('createCategoryFromExport error', error);
    return null;
  }
}

function buildPermissionOverwrites(guild, factionRoleId, exportedRoleIds = {}) {
  const overwrites = [
    { id: guild.id, deny: [PermissionFlagsBits.ViewChannel] }
  ];

  overwrites.push({
    id: factionRoleId,
    allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.SendMessages, PermissionFlagsBits.Connect]
  });

  if (exportedRoleIds.hicommRole) overwrites.push({ id: exportedRoleIds.hicommRole, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] });
  if (exportedRoleIds.midcommRole) overwrites.push({ id: exportedRoleIds.midcommRole, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] });
  if (exportedRoleIds.directorRole) overwrites.push({ id: exportedRoleIds.directorRole, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.ManageMessages] });
  if (exportedRoleIds.ownerRole) overwrites.push({ id: exportedRoleIds.ownerRole, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ManageChannels, PermissionFlagsBits.ManageMessages] });

  return overwrites;
}

async function importFromExportJson(interaction, exportJson, options = { dryRun: false }) {
  if (!exportJson || !Array.isArray(exportJson.factions)) {
    throw new Error('Invalid export format: no factions[] found');
  }

  const guild = interaction.guild;
  const importedFactions = [];
  const warnings = [];
  const errors = [];

  for (const f of exportJson.factions) {
    try {
      const factionInfo = f.faction_info || {};
      const uniqueId = factionInfo.unique_id;
      if (!uniqueId) {
        warnings.push(`Skipping faction with missing unique_id`);
        continue;
      }
      const existing = await new Promise((res, rej) => db.get("SELECT * FROM factions WHERE unique_id = ?", [uniqueId], (err, row) => err ? rej(err) : res(row)));
      if (existing) {
        if (!options.dryRun) {
          try {
            if (existing.category_id) {
              const cat = await guild.channels.fetch(existing.category_id).catch(() => null);
              if (cat) await cat.delete().catch(() => null);
            }
            if (existing.role_id) {
              const role = await guild.roles.fetch(existing.role_id).catch(() => null);
              if (role) await role.delete().catch(() => null);
            }
          } catch(e) { console.error('error deleting existing discord resources', e); }

          await deleteFaction(existing.id);
        } else {
          warnings.push(`(dry-run) Would delete existing faction ${uniqueId}`);
        }
      }

      let newFactionId = null;
      if (!options.dryRun) {
        await new Promise((resolve, reject) => {
          db.run(`INSERT INTO factions (name, unique_id, type, roblox_group, color, image_url, owner_id, role_id, hicomm_role_id, director_role_id, midcomm_role_id, category_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, 
            [factionInfo.name, factionInfo.unique_id, factionInfo.type, factionInfo.roblox_group, factionInfo.color, factionInfo.image_url || null, factionInfo.owner_id || null, null, null, null, null, null],
            function(err) { if (err) reject(err); else { newFactionId = this.lastID; resolve(); } });
        });
      }

      let factionRole = null;
      let category = null;
      if (!options.dryRun) {
        try {
          const roleExport = f.discord_objects?.role || { name: `${factionInfo.name} | Member`, color: factionInfo.color };
          factionRole = await createRoleFromExport(guild, roleExport, factionInfo.name || factionInfo.unique_id);
          if (factionRole && newFactionId) await new Promise((res, rej) => db.run("UPDATE factions SET role_id = ? WHERE id = ?", [factionRole.id, newFactionId], (err) => err ? rej(err) : res()));
        } catch (e) {
          console.error('role creation error', e);
          warnings.push(`Role creation failed for ${uniqueId}: ${e.message}`);
        }

        try {
          const catExport = f.discord_objects?.category || { name: `〡${factionInfo.name || factionInfo.unique_id}` };
          category = await createCategoryFromExport(guild, catExport, factionInfo.name || factionInfo.unique_id);
          if (category && newFactionId) await new Promise((res, rej) => db.run("UPDATE factions SET category_id = ? WHERE id = ?", [category.id, newFactionId], (err) => err ? rej(err) : res()));
        } catch (e) {
          console.error('category creation error', e);
          warnings.push(`Category creation failed for ${uniqueId}: ${e.message}`);
        }
      }

      const exportedChannels = Array.isArray(f.channels) ? f.channels : [];
      const createdChannels = [];
      for (const ch of exportedChannels) {
        const chName = ch.channel_name || ch.name || `channel-${Math.random().toString(36).slice(2,7)}`;
        const isVoice = (ch.channel_type && ch.channel_type.toLowerCase().includes('voice')) || ch.type === 'voice' || ch.key === 'voice';
        if (!options.dryRun) {
          try {
            const overwrites = buildPermissionOverwrites(guild, factionRole?.id, {});
            const created = await guild.channels.create({ name: chName, type: isVoice ? 2 : 0, parent: category ? category.id : null, permissionOverwrites: overwrites });
            await new Promise((res, rej) => db.run("INSERT INTO faction_channels (faction_id, channel_type, channel_id, channel_name) VALUES (?, ?, ?, ?)", [newFactionId, ch.channel_type || ch.key || 'text', created.id, chName], (err) => err ? rej(err) : res()));
            createdChannels.push(created);
          } catch (err) {
            console.error('channel create error', err);
            warnings.push(`Failed to create channel ${chName} for faction ${uniqueId}`);
          }
        } else {
          createdChannels.push({ name: chName, type: ch.channel_type || 'text' });
        }
      }

      const exportedMembers = Array.isArray(f.members) ? f.members : [];
      const missingMembers = [];
      for (const mem of exportedMembers) {
        if (!options.dryRun) {
          try {
            await new Promise((res, rej) => db.run("INSERT OR REPLACE INTO faction_members (faction_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)", [newFactionId, mem.user_id, mem.role || 'member', mem.joined_at || null], (err) => err ? rej(err) : res()));
          } catch (err) {
            console.error('insert member db', err);
            warnings.push(`DB insert member failed for ${mem.user_id} in ${uniqueId}`);
          }

          const guildMember = await guild.members.fetch(mem.user_id).catch(() => null);
          if (guildMember) {
            try {
              if (factionRole) await guildMember.roles.add(factionRole.id).catch(() => null);
              const desiredRoleName = `${factionInfo.name} | ${mem.role ? mem.role.charAt(0).toUpperCase() + mem.role.slice(1) : 'Member'}`;
              const matched = guild.roles.cache.find(r => r.name === desiredRoleName);
              if (matched) await guildMember.roles.add(matched.id).catch(() => null);
              try { if (guild.members.me.permissions.has(PermissionFlagsBits.ManageNicknames)) await guildMember.setNickname(`[${factionInfo.unique_id}] ${guildMember.user.username}`).catch(() => null); } catch(e){}
            } catch (err) {
              console.error('assign role error', err);
              warnings.push(`Could not assign faction role to ${mem.user_id}`);
            }
          } else {
            missingMembers.push(mem.user_id);
          }
        } else {
          if (!mem.user_id) missingMembers.push(mem.user_id);
        }
      }

      const exportedStrikes = Array.isArray(f.strikes) ? f.strikes : [];
      for (const s of exportedStrikes) {
        if (!options.dryRun) {
          try {
            await new Promise((res, rej) => db.run("INSERT INTO faction_strikes (faction_id, user_id, reason, given_by, created_at) VALUES (?, ?, ?, ?, ?)", [newFactionId, s.user_id, s.reason, s.given_by, s.created_at || new Date().toISOString()], (err) => err ? rej(err) : res()));
          } catch (err) {
            console.error('insert strike', err);
            warnings.push(`Failed to insert strike for ${s.user_id} in ${uniqueId}`);
          }
        }
      }

      const exportedBlacklist = Array.isArray(f.blacklist) ? f.blacklist : [];
      for (const b of exportedBlacklist) {
        if (!options.dryRun) {
          try {
            await new Promise((res, rej) => db.run("INSERT INTO faction_blacklist (faction_id, user_id, reason, blacklisted_by, created_at) VALUES (?, ?, ?, ?, ?)", [newFactionId, b.user_id, b.reason, b.blacklisted_by, b.created_at || new Date().toISOString()], (err) => err ? rej(err) : res()));
          } catch (err) {
            console.error('insert blacklist', err);
            warnings.push(`Failed to insert blacklist entry for ${b.user_id} in ${uniqueId}`);
          }
        }
      }

      const exportedRelations = f.relations || {};
      for (const otherId in exportedRelations) {
        const rel = exportedRelations[otherId];
        if (!options.dryRun) {
          try {
            const otherFaction = await new Promise((res, rej) => db.get("SELECT id FROM factions WHERE unique_id = ?", [otherId], (err,row) => err ? rej(err) : res(row)));
            if (otherFaction) {
              await new Promise((res, rej) => db.run("INSERT OR REPLACE INTO faction_relations (faction1_id, faction2_id, relation_type) VALUES (?, ?, ?)", [newFactionId, otherFaction.id, rel.type], (err) => err ? rej(err) : res()));
            } else {
              warnings.push(`Relation target not found for ${otherId} while importing ${uniqueId}`);
            }
          } catch (err) {
            console.error('insert relation', err);
            warnings.push(`Failed to insert relation ${otherId} for ${uniqueId}`);
          }
        }
      }

      if (f.permissions && !options.dryRun && typeof updateFactionPermissions === 'function') {
        try {
          await updateFactionPermissions(newFactionId, {
            rename: f.permissions.rename ?? DEFAULT_PERMISSIONS.rename,
            kick: f.permissions.kick ?? DEFAULT_PERMISSIONS.kick,
            promote: f.permissions.promote ?? DEFAULT_PERMISSIONS.promote,
            demote: f.permissions.demote ?? DEFAULT_PERMISSIONS.demote,
            invite: f.permissions.invite ?? DEFAULT_PERMISSIONS.invite,
            strike: f.permissions.strike ?? DEFAULT_PERMISSIONS.strike,
            leave: f.permissions.leave ?? DEFAULT_PERMISSIONS.leave
          });
        } catch (err) {
          warnings.push(`Failed to set permissions for ${uniqueId}`);
        }
      }

      importedFactions.push({ unique_id: uniqueId, name: factionInfo.name, createdChannels: createdChannels.length, members: exportedMembers.length, missingMembers });
    } catch (err) {
      console.error('faction import error', err);
      errors.push(err.message || String(err));
    }
  } 

  return { importedFactions, warnings, errors };
}

client.once('ready', async () => {
  try {
    await client.application.commands.create({
      name: 'import',
      description: 'Import faction data from an export JSON file (OVERWRITE existing factions)',
      options: [{ name: 'file', description: 'Export JSON file', type: 11, required: true }]
    });
    await client.application.commands.create({
      name: 'import-dryrun',
      description: 'Validate an export JSON file without writing (dry-run)',
      options: [{ name: 'file', description: 'Export JSON file', type: 11, required: true }]
    });
    console.log('Import commands registered');
  } catch (e) {
    console.error('Failed to register import commands', e);
  }
});

client.on('interactionCreate', async (interaction) => {
  try {
    if (!interaction.isChatInputCommand()) return;
    if (interaction.commandName !== 'import' && interaction.commandName !== 'import-dryrun') return;

    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator)) {
      return interaction.reply({ content: "❌ You need Administrator permissions to use /import.", ephemeral: true });
    }

    await interaction.deferReply({ ephemeral: true });

    const attachment = interaction.options.getAttachment('file');
    if (!attachment) return interaction.editReply({ content: '❌ No file provided.' });

    let json;
    try {
      json = await fetchJsonFromAttachment(attachment.url);
    } catch (e) {
      return interaction.editReply({ content: `❌ Failed to download or parse JSON: ${e.message}` });
    }

    const dryRun = (interaction.commandName === 'import-dryrun');
    try {
      const result = await importFromExportJson(interaction, json, { dryRun });
      const summaryEmbed = new EmbedBuilder()
        .setColor(dryRun ? 0xFFFF00 : 0x00FF00)
        .setTitle(dryRun ? '🔍 Import Dry-Run Summary' : '✅ Import Complete')
        .setDescription(dryRun ? 'Dry-run validated the file — no changes made.' : 'Imported factions from file.')
        .setTimestamp();

      summaryEmbed.addFields(
        { name: 'Factions Processed', value: result.importedFactions.length.toString(), inline: true },
        { name: 'Warnings', value: result.warnings.length.toString(), inline: true },
        { name: 'Errors', value: result.errors.length.toString(), inline: true }
      );

      if (result.importedFactions.length > 0) {
        summaryEmbed.addFields({
          name: 'Examples',
          value: result.importedFactions.slice(0,5).map(f => `• ${f.name || 'Unnamed'} [${f.unique_id}] — channels: ${f.createdChannels}, members: ${f.members}, missing: ${f.missingMembers.length}`).join('\n'),
          inline: false
        });
      }
      if (result.warnings.length) summaryEmbed.addFields({ name: 'Warnings (first 5)', value: result.warnings.slice(0,5).join('\n'), inline: false });
      if (result.errors.length) summaryEmbed.addFields({ name: 'Errors (first 5)', value: result.errors.slice(0,5).join('\n'), inline: false });

      await interaction.editReply({ embeds: [summaryEmbed] });
    } catch (err) {
      console.error('import fatal error', err);
      await interaction.editReply({ content: `❌ Fatal error while importing: ${err.message || String(err)}` });
    }
  } catch (err) {
    console.error('interaction import handler error', err);
    try { if (interaction.deferred || interaction.replied) await interaction.followUp({ content: '❌ An internal error occurred.', ephemeral: true }); else await interaction.reply({ content: '❌ An internal error occurred.', ephemeral: true }); } catch {}
  }
});
