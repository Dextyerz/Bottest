import logging
from datetime import datetime

import texttable
import discord.utils
from dateutil import parser
from aiosqlite import IntegrityError
from discord.errors import Forbidden
from discord.ext import commands, tasks

from helpers import misc
from helpers.paginator import Paginator
from helpers.converters import positive_integer, license_duration
from helpers.errors import RoleNotFound, DatabaseMissingData, GuildNotFound
from helpers.embed_handler import success, warning, failure, info, simple_embed
from helpers.licence_helper import construct_expiration_date, get_remaining_time, get_current_time

logger = logging.getLogger(__name__)


class LicenseHandler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.license_check.start()

    @tasks.loop(seconds=60.0)
    async def license_check(self):
        try:
            await self.check_all_active_licenses()
        except Exception as e:
            logger.critical(e)

    @license_check.before_loop
    async def before_printer(self):
        logger.info("Starting license check loop..")
        await self.bot.wait_until_ready()
        logger.info("License check loop started!")

    async def check_all_active_licenses(self):
        """
        Checks all active member licenses in database and if license is expired then remove
        the role from member and send some message.

        TODO: Move query to database handler
        """
        async with self.bot.main_db.connection.execute("SELECT * FROM LICENSED_MEMBERS") as cursor:
            async for row in cursor:
                member_id = int(row[0])
                member_guild_id = int(row[1])
                expiration_date = parser.parse(row[2])
                licensed_role_id = int(row[3])
                if await LicenseHandler.has_license_expired(expiration_date):
                    logger.info(f"Expired license for member:{member_id} role:{licensed_role_id} guild:{member_guild_id}")
                    try:
                        await self.remove_role(member_id, member_guild_id, licensed_role_id)
                    except RoleNotFound as e1:
                        logger.warning(e1)
                        logger.warning(f"Role expired but can't be removed from member because he doesn't have it! "
                                       f"Someone must have manually removed it before it expired.\t"
                                       f"Member ID:{member_id}, guild ID:{member_guild_id}, role ID:{licensed_role_id}"
                                       f"Continuing to db entry removal...")
                    except GuildNotFound as e2:
                        # If guild is not found log it and continue to guild database deletion
                        logger.warning(e2)
                        logger.warning(f"Guild {member_guild_id} saved in database but not found in bot guilds!"
                                       "Removing all entries of it from database!")
                        await self.bot.main_db.remove_all_guild_data(member_guild_id, guild_table_too=True)
                        logger.info(f"Successfully deleted all database data for guild {member_guild_id}")
                        continue
                    except Exception as e3:
                        logger.warning(f"Can't remove role {licensed_role_id } from member {member_id } guild {member_guild_id }, ignoring error: {e3}")
                        continue
                    await self.bot.main_db.delete_licensed_member(member_id, licensed_role_id)
                    logger.info(f"Role {licensed_role_id} successfully removed from member:{member_id}")

@staticmethod
async def has_license_expired(expiration_date: datetime) -> bool:
    """
    Check if param expiration date is in the past related to the current date.
    If it is in the past, then the license is considered expired.
    :param expiration_date: datetime object
    :return: True if license is expired, False otherwise
    """
    if expiration_date < get_current_time():
        # Expired
        return True
    else:
        return False

async def remove_role(self, member_id, guild_id, licensed_role_id):
    """
    Removes the specified role from the member based on the params
    :param member_id: unique member id
    :param guild_id: guild ID from where the member is from. Needed because a member can be in
                     multiple guilds at the same time.
    :param licensed_role_id: ID of a role to remove from the member
    :raise RoleNotFound: if the role to be removed isn't in the member's roles (case when in db it's saved but someone
            manually removed their role so when the db role expires and needs to be removed, there is nothing to be
            removed)
    """
    guild = self.bot.get_guild(guild_id)
    if guild is None:
        raise GuildNotFound(f"Fatal exception. "
                            f"Guild **{guild_id}** loaded from the database cannot be found in bot guilds!")
    member = guild.get_member(member_id)
    if member is None:
        member = await guild.fetch_member(member_id)
    if member is None:
        logger.warning(f"Can't remove licensed role {licensed_role_id} from member {member_id} "
                       f"because he has left the guild {licensed_role_id} ({guild}).")
        return
    member_role = discord.utils.get(member.roles, id=licensed_role_id)
    if member_role is None:
        raise RoleNotFound(f"Can't remove licensed role {member_role} for {member.mention}."
                           f"Role not found ")
    else:
        await member.remove_roles(member_role)
        try:
            expired = f"Your license in guild **{guild}** has expired for the following role: **{member_role}** "
            await member.send(embed=simple_embed(expired, "Notification", discord.Colour.blue()))
        except Forbidden:
            # Ignore if the user has blocked DM
            pass

@commands.Cog.listener()
async def on_guild_join(self, guild):
    logger.info(f"Guild {guild.name} {guild.id} joined.")
    guild_id = guild.id
    default_prefix = self.bot.config["default_prefix"]
    await self.bot.main_db.setup_new_guild(guild_id, default_prefix)
    logger.info(f"Guild {guild.name} {guild.id} database data added.")

@commands.Cog.listener()
async def on_guild_remove(self, guild):
    guild_id = guild.id
    logger.info(f"Guild '{guild.name}'' {guild.id} was removed. Removing all database entries.")
    await self.bot.main_db.remove_all_guild_data(guild_id, guild_table_too=True)
    logger.info(f"Guild '{guild.name}'' {guild.id} all database entries successfully removed.")

@commands.Cog.listener()
async def on_guild_role_delete(self, role):
    guild = role.guild
    logger.info(f"Role '{role.name}'' {role.id} was removed from guild '{guild.name}'' {guild.id}. "
                f"Removing all database entries.")
    await self.bot.main_db.remove_all_guild_role_data(role.id)

@commands.Cog.listener()
async def on_member_update(self, before, after):
    if len(before.roles) > len(after.roles):
        removed_roles_list = list(set(before.roles) - set(after.roles))
        for role in removed_roles_list:
            await self.bot.main_db.delete_licensed_member(before.id, role.id)


@commands.command()
@commands.bot_has_permissions(manage_roles=True)
@commands.has_permissions(manage_roles=True)
@commands.guild_only()
async def revoke(self, ctx, member: discord.Member, role: discord.Role):
    """
    Revoke active subscription from a member.
    Removes both the database entry and the role from a member.
    """
    try:
        # DRY
        await self.bot.main_db.get_member_license_expiration_date(member.id, role.id)
    except DatabaseMissingData:
        msg = f"{member.mention} doesn't have a subscription for {role.mention} saved in the database!"
        await ctx.send(embed=failure(msg))
        return
    # First remove the role from the member because this can fail in case of changed role hierarchy.
    await member.remove_roles(role)
    await self.bot.main_db.delete_licensed_member(member.id, role.id)
    msg = f"Successfully revoked subscription for {role.mention} from {member.mention}"
    await ctx.send(embed=success(msg, ctx.me))
    logger.info(f"{ctx.author} is revoking subscription for role {role} from member {member} in guild {ctx.guild}")

@commands.command()
@commands.bot_has_permissions(manage_roles=True)
@commands.has_permissions(manage_roles=True)
@commands.guild_only()
async def revoke_all(self, ctx, member: discord.Member):
    """
    Revoke ALL active subscriptions from a member.
    Removes both the database entry and the role from a member.
    """
    member_data = await self.bot.main_db.get_member_data(ctx.guild.id, member.id)
    count = 0
    for tple in member_data:
        role_id = int(tple[0])
        role = ctx.guild.get_role(role_id)
        if role is None:
            logger.info(f"'revoke_all' called in guild {ctx.guild} and role that's loaded from the database with "
                        f"ID:{role_id} cannot be removed from {member} because it doesn't exist in the guild anymore! "
                        f"Continuing to removal from the database.")
            await self.bot.main_db.delete_licensed_member(member.id, role_id)
            count += 1
        else:
            try:
                await member.remove_roles(role)
                await self.bot.main_db.delete_licensed_member(member.id, role_id)
                count += 1
            except Forbidden as e:
                msg = (f"Can't remove {role.mention} from {member.mention}, no permissions to manage that role as "
                       f" I can only manage roles below me in the hierarchy. This probably means that {role.mention} "
                       f"was moved up in the hierarchy **after** it was registered in my system "
                       f"(or mine was moved down).\n"
                       f"{e}")
                await ctx.send(embed=failure(msg))
    if count:
        msg = f"Successfully revoked {count} subscriptions from {member.mention}!"
        await ctx.send(embed=success(msg, ctx.me))
        logger.info(f"{ctx.author} has revoked all subscriptions for member {member} in guild {ctx.guild}")
    else:
        msg = f"Couldn't revoke even a single subscription from member {member.mention}!"
        await ctx.send(embed=warning(msg))

@commands.command(aliases=["authorize", "activate"])
async def redeem(self, ctx, license):
    """
    Adds a role to the member who invoked the command.
    If the license is valid, get the role linked to it and assign the role to the member who invoked the command.
    Removes the license from the database (it was redeemed).
    TODO: Better security (right now the license is visible in plain sight in the guild)
    """
    license_data = await self.bot.main_db.get_license_data(license)
    if license_data is None:
        await ctx.send(embed=failure("The license key you entered is invalid/deactivated."))
        return
    license_guild_id, license_role_id = license_data
    await self.activate_license(ctx, license, license_guild_id, license_role_id, ctx.author)

@commands.command(aliases=["add_license"])
@commands.has_permissions(manage_roles=True)
async def add_license(self, ctx, license, member: discord.Member):
    """Manually add a license to a member."""
    license_data = await self.bot.main_db.get_license_data(license)
    if license_data is None:
        await ctx.send(embed=failure("The license key you entered is invalid/deactivated."))
        return
    license_guild_id, license_role_id = license_data
    await self.activate_license(ctx, license, license_guild_id, license_role_id, member)
    logger.info(f"{ctx.author} is adding license {license} to member {member} in guild {ctx.guild}")

async def activate_license(self, ctx, license, guild_id: int, role_id: int, member):
    """
    :param ctx: invoked context
    :param guild_id: guild id tied to license
    :param role_id: role id tied to license
    :param license: license to add
    :param member: who to give the role to. Union[User, Member] depending on if called in guild or DM
    """
    guild = self.bot.get_guild(guild_id)
    if guild is None:
        await ctx.send(embed=failure("Guild tied to that license not found in bot guilds."))
        return
    if ctx.guild is not None and ctx.guild.id != guild_id:
        await ctx.send(embed=failure(f"That license is not for this guild! "
                                     f"Either redeem it in the correct guild '{guild.name}' or redeem it in bot DM."))
        return
    # Decorator won't work in DM, so have to manually check
    if not guild.me.guild_permissions.manage_roles:
        await ctx.send(embed=failure(f"Guild '{guild.name}' , can't assign roles - no manage roles permission."))
        if ctx.guild is not None:
            # delete message but only if in guild, can't delete DM messages
            await ctx.message.delete()
        return
    # Passed member can be a user if redeem was activated in DM, so get the member
    if ctx.guild is None:
        member = guild.get_member(member.id)
        if member is None:
            await ctx.send(embed=failure("You are no longer in the guild you're trying to activate the license for!"))
            return
    if await self.bot.main_db.is_valid_license(license, guild.id):
        # Adding the role to the member requires that role object
        # First, we get the role linked to the license
        role = guild.get_role(role_id)
        if role is None:
            log_error_msg = (f"Can't find role {role_id} in guild {guild.id} '{guild.name}' "
                             f"from license: '{license}' member to give the role to: {member.id} '{member.name}'"
                             "\n\nProceeding to delete this invalid license from the database!")
            logger.critical(log_error_msg)
            msg = ("Well, this is awkward...\n\n"
                   "The role that was supposed to be given out by this license has been deleted from this guild!"
                   f"\n\nError message:\n\n{log_error_msg}")
            await ctx.send(embed=failure(msg))
            await self.bot.main_db.delete_license(license)
            return
        if role in member.roles:
            try:
                expiration_date = await self.bot.main_db.get_member_license_expiration_date(member.id, role_id)
            except DatabaseMissingData as e:
                # TODO print role name instead of ID (from e)
                msg = e.message
                msg += (f"\nThe bot did not register {member.mention} in the database with that role, but somehow they have it."
                        "\nThis probably means that they were manually assigned this role without using the bot license system."
                        "\nHave someone remove the role from them and call this command again.")
                await ctx.send(embed=failure(msg))
                if ctx.guild is not None:
                    # delete message but only if in guild, can't delete DM messages
                    await ctx.message.delete()
                return
            remaining_time = get_remaining_time(expiration_date)
            msg = (f"{member.mention} already has an active subscription for the '{role.name}' role!"
                   f"\nIt's valid for another {remaining_time}")
            await ctx.send(embed=warning(msg))
            if ctx.guild is not None:
                await ctx.message.delete()
            return
        await member.add_roles(role, reason="Redeemed license.")
        license_duration = await self.bot.main_db.get_license_duration_hours(license)
        expiration_date = construct_expiration_date(license_duration)
        try:
            await self.bot.main_db.add_new_licensed_member(member.id, guild.id, expiration_date, role_id)
        except IntegrityError:
            await self.bot.main_db.delete_licensed_member(member.id, role_id)
            await self.bot.main_db.add_new_licensed_member(member.id, guild.id, expiration_date, role_id)
            msg = (f"Someone removed the role manually from {member.mention}, but no worries,\n"
                   "since the license is valid we're just gonna reactivate it :)")
            await ctx.send(embed=info(msg, ctx.me))
        # Remove the guild license from the database, so it can't be redeemed again
        await self.bot.main_db.delete_license(license)
        # Send a message notifying the user
        msg = f"License valid - guild '{guild.name}' adding role '{role.name}' to {member.mention} for a duration of {license_duration}h"
        await ctx.send(embed=success(msg, ctx.me))
    else:
        await ctx.send(embed=failure("The license key you entered is invalid/deactivated."))

@commands.command()
@commands.cooldown(1, 10, commands.BucketType.guild)
@commands.has_permissions(administrator=True)
@commands.guild_only()
async def generate(self, ctx, num: positive_integer = 3, license_role: discord.Role = None,
                   *, license_duration: license_duration = None):
    """
    Generates new guild licenses.
    The maximum number of licenses to generate at once is 25.
    All arguments are optional. If not passed, default guild values are used.
    Arguments are stacked, meaning you can't pass 'license_duration' without the first 2 arguments.
    On the other hand, you can pass only 'num'.
    Example usages:
    generate
    generate 10
    generate 5 @role
    generate 7 @role 1w
    License duration is either a number representing hours or a string consisting of words in the format:
    each word has to contain [integer][format], entries are separated by a space.
    Formats are:
    years y months m weeks w days d hours h
    License duration examples:
    20
    2y 5months
    1m
    3d 12h
    1w 2m 1w
    1week 1week
    12hours 5d
    ...
    """
    if num > 25:
        await ctx.send(embed=failure("The maximum number of licenses to generate at once is 25."))
        return
    if license_role is not None and not ctx.me.top_role > license_role:
        await ctx.send(embed=failure("I can only manage roles **below** me in the hierarchy."))
        return
    guild_id = ctx.guild.id
    max_licenses_per_guild = self.bot.config["maximum_unused_guild_licences"]
    guild_licenses_count = await self.bot.main_db.get_guild_license_total_count(guild_id)
    if guild_licenses_count == max_licenses_per_guild:
        msg = f"You have reached the maximum number of unused licenses per guild: {max_licenses_per_guild}!"
        await ctx.send(embed=warning(msg))
        return
    if guild_licenses_count + num > max_licenses_per_guild:
        msg = (f"I can't generate since you will exceed the limit of {max_licenses_per_guild} licenses!\n"
               f"Remaining licenses to generate: {max_licenses_per_guild - guild_licenses_count}.")
        await ctx.send(embed=failure(msg))
        return
    if license_duration is None:
        license_duration = await self.bot.main_db.get_default_guild_license_duration_hours(guild_id)
    if license_role is None:
        licensed_role_id = await self.bot.main_db.get_default_guild_license_role_id(guild_id)
        license_role = ctx.guild.get_role(licensed_role_id)
        if license_role is None:
            await self.handle_missing_default_role(ctx, licensed_role_id)
            return
        generated = await self.bot.main_db.generate_guild_licenses(num, guild_id, licensed_role_id, license_duration)
    else:
        generated = await self.bot.main_db.generate_guild_licenses(num, guild_id, license_role.id, license_duration)
    count_generated = len(generated)
    ctx_msg = (f"Successfully generated {count_generated} licenses for role {license_role.mention}"
               f" in duration of {license_duration}h.\n"
               f"Sending generated licenses in DM for quick use.")
    await ctx.send(embed=success(ctx_msg, ctx.me))
    table = texttable.Texttable(max_width=45)
    table.set_cols_dtype(["t"])
    table.set_cols_align(["c"])
    header = ("License",)
    table.add_row(header)
    for license in generated:
        table.add_row((license,))
    dm_msg = (f"Generated {count_generated} licenses for role '{license_role.name}' in "
              f"guild '{ctx.guild.name}' in duration of {license_duration}h:\n"
              f"{table.draw()}")
    await ctx.author.send(f"```{misc.maximize_size(dm_msg)}```")


@commands.command(aliases=["licences"])
@commands.cooldown(1, 10, commands.BucketType.guild)
@commands.has_permissions(administrator=True)
@commands.guild_only()
async def licenses(self, ctx, license_role: discord.Role = None):
    """
    Shows all licenses for a role in DM.
    Shows licenses linked to license_role and your guild.
    If license_role is not passed, then the default guild role is used.
    Sends results in DM to the user who invoked the command.
    """
    num = self.bot.config["maximum_unused_guild_licences"]
    guild_id = ctx.guild.id
    if license_role is None:
        # If the license role is not passed, just use the guild's default license role
        # We load it from the database
        licensed_role_id = await self.bot.main_db.get_default_guild_license_role_id(guild_id)
        license_role = ctx.guild.get_role(licensed_role_id)
        if license_role is None:
            await self.handle_missing_default_role(ctx, licensed_role_id)
            return
        to_show = await self.bot.main_db.get_guild_licenses(num, guild_id, licensed_role_id)
    else:
        to_show = await self.bot.main_db.get_guild_licenses(num, guild_id, license_role.id)
    if len(to_show) == 0:
        await ctx.send(embed=failure("No available licenses for that role."))
        return
    table = texttable.Texttable(max_width=60)
    table.set_cols_dtype(["t", "t"])
    table.set_cols_align(["c", "c"])
    header = ("License", "Duration(h)")
    table.add_row(header)
    for tple in to_show:
        table.add_row((tple[0], tple[1]))
    dm_title = f"Showing {len(to_show)} licenses for role '{license_role.name}' in guild '{ctx.guild.name}':\n\n"
    await ctx.send(embed=success("Sent to DM!", ctx.me), delete_after=5)
    await Paginator.paginate(self.bot, ctx.author, ctx.author, table.draw(), title=dm_title)

@commands.command(aliases=["random_licenses"])
@commands.cooldown(1, 10, commands.BucketType.guild)
@commands.has_permissions(administrator=True)
@commands.guild_only()
async def random_license(self, ctx, number: int = 10):
    """
    Shows random guild licenses in DM.
    If the number is not passed, the default value is 10.
    The maximum number of licenses to show is 100.
    Sends results in DM to the user who invoked the command.
    """
    maximum_number = self.bot.config["maximum_unused_guild_licences"]
    if number > maximum_number:
        await ctx.send(embed=failure(f"The number can't be larger than {maximum_number}!"))
        return
    to_show = await self.bot.main_db.get_random_licenses(ctx.guild.id, number)
    if not to_show:
        await ctx.send(embed=failure("No licenses saved in the database."))
        return
    table = texttable.Texttable(max_width=90)
    table.set_cols_dtype(["t", "t", "t"])
    table.set_cols_align(["c", "c", "c"])
    header = ("License", "Role", "Duration (h)")
    table.add_row(header)
    for entry in to_show:
        try:
            role = ctx.guild.get_role(int(entry[1]))
            table.add_row((entry[0], role.name, entry[2]))
        except (ValueError, AttributeError):
            table.add_row(entry)
    title = f"Showing {number} random licenses from guild '{ctx.guild.name}':\n\n"
    await ctx.send(embed=success("Sent to DM!", ctx.me), delete_after=5)
    await Paginator.paginate(self.bot, ctx.author, ctx.author, table.draw(), title=title)


@commands.command(aliases=["data"])
@commands.guild_only()
async def member_data(self, ctx, member: discord.Member = None):
    """
    Shows active subscriptions of a member.
    Sends the result in DMs.
    """
    if member is None:
        member = ctx.author
    else:
        if ctx.author == member:
            pass
        elif not ctx.author.guild_permissions.administrator:
            await ctx.send(embed=failure("You need administrator permission to see other members' data."))
            return
    table = texttable.Texttable(max_width=90)
    table.set_cols_dtype(["t", "t"])
    table.set_cols_align(["c", "c"])
    header = ("Licensed role", "Expiration date")
    table.add_row(header)
    all_active = await self.bot.main_db.get_member_data(ctx.guild.id, member.id)
    if not all_active:
        await ctx.send(embed=failure(f"Nothing to show for {member.mention}."))
        return
    for entry in all_active:
        try:
            role = ctx.guild.get_role(int(entry[0]))
            table.add_row((role.name, entry[1]))
        except (ValueError, AttributeError):
            table.add_row(entry)
    local_time = get_current_time()
    title = (f"Server local time: {local_time}\n\n"
             f"{member.name}'s active subscriptions in guild '{ctx.guild.name}':\n\n")
    await ctx.send(embed=info("Sent in DMs!", ctx.me), delete_after=5)
    await Paginator.paginate(self.bot, ctx.author, ctx.author, table.draw(), title=title, prefix="```DNS\n")


@commands.command()
@commands.has_permissions(administrator=True)
@commands.guild_only()
async def delete_license(self, ctx, license):
    """Deletes the specified stored license."""
    if await self.bot.main_db.is_valid_license(license, ctx.guild.id):
        await self.bot.main_db.delete_license(license)
        await ctx.send(embed=success("License deleted.", ctx.me))
        logger.info(f"{ctx.author} is deleting license {license} from guild {ctx.guild}")
    else:
        await ctx.send(embed=failure("License is not valid."))


@commands.command()
@commands.has_permissions(administrator=True)
@commands.guild_only()
async def delete_all(self, ctx):
    """
    Deletes all stored guild licenses.
    You will have to reply with "yes" for confirmation.
    """
    def check(msg):
        return msg.author == ctx.author and msg.channel == ctx.channel and msg.content == "yes"

    await ctx.send(embed=warning("Are you sure? Reply with case-sensitive `yes` in the next 15 seconds to proceed."))
    await self.bot.wait_for("message", check=check, timeout=15)
    await self.bot.main_db.remove_all_stored_guild_licenses(ctx.guild.id)
    await ctx.send(embed=success("Done!", ctx.me))


async def handle_missing_default_role(self, ctx, missing_role_id: int):
    """
    Guilds have a default license role that will be used if no role argument is
    passed when generating licenses. But it can happen that that role gets
    deleted while it's still in the database (similar problem as in check_all_active_licenses)
    :param missing_role_id: role that is in the database but is missing in the guild
    TODO: on startup/reconnect check if the default role from the database is valid
    """
    msg = (f"Trying to use the role with ID {missing_role_id} that was set "
           f"as the default role for guild {ctx.guild.name} but cannot find it "
           f"anymore in the list of roles!\n\n"
           f"It's saved in the database but it looks like it was deleted from the guild.\n"
           f"Please update it.")
    await ctx.send(embed=failure(msg))


def setup(bot):
    bot.add_cog(LicenseHandler(bot))