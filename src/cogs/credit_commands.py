import discord
from discord import app_commands
from discord.ext import commands
import os

class CreditCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.admin_user_id = int(os.getenv('ADMIN_USER_ID'))

    def is_admin():
        async def predicate(interaction: discord.Interaction):
            return interaction.user.id == int(os.getenv('ADMIN_USER_ID'))
        return app_commands.check(predicate)

    @app_commands.command(name="balance", description="Check your current credit balance.")
    async def balance(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        try:
            credits = await self.bot.credit_system.get_credits(user_id)
            await interaction.response.send_message(f"{interaction.user.mention}, you have **{credits}** credits.")
        except Exception as e:
            self.bot.logger.error(f"Error fetching credits for user {user_id}: {e}", exc_info=True)
            await interaction.response.send_message("An error occurred while fetching your credits. Please try again later.")

    @app_commands.command(name="addcredit", description="Add credits to a user (Admin only).")
    @is_admin()
    async def add_credit(self, interaction: discord.Interaction, member: discord.Member, amount: int):
        if amount <= 0:
            await interaction.response.send_message("Please enter a positive amount of credits to add.")
            return
        try:
            await self.bot.credit_system.add_credit(member.id, amount)
            await interaction.response.send_message(f"Added **{amount}** credits to {member.mention}.")
        except Exception as e:
            self.bot.logger.error(f"Error adding credits to user {member.id}: {e}", exc_info=True)
            await interaction.response.send_message("An error occurred while adding credits. Please try again later.")

    @app_commands.command(name="deductcredit", description="Deduct credits from a user (Admin only).")
    @is_admin()
    async def deduct_credit(self, interaction: discord.Interaction, member: discord.Member, amount: int):
        if amount <= 0:
            await interaction.response.send_message("Please enter a positive amount of credits to deduct.")
            return
        try:
            success = await self.bot.credit_system.deduct_credit(member.id, amount)
            if success:
                await interaction.response.send_message(f"Deducted **{amount}** credits from {member.mention}.")
            else:
                await interaction.response.send_message(f"{member.mention} does not have enough credits.")
        except Exception as e:
            self.bot.logger.error(f"Error deducting credits from user {member.id}: {e}", exc_info=True)
            await interaction.response.send_message("An error occurred while deducting credits. Please try again later.")

    @app_commands.command(name="claim", description="Claim 5 daily credits.")
    async def claim(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        try:
            can_claim, remaining = await self.bot.credit_system.can_claim(user_id)
            if can_claim:
                await self.bot.credit_system.add_credit(user_id, 5)
                await self.bot.credit_system.set_last_claim(user_id)
                await interaction.response.send_message(f"{interaction.user.mention}, you've successfully claimed **5** credits! You can claim again in 24 hours.")
            else:
                hours, remainder = divmod(remaining, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_remaining = ""
                if hours > 0:
                    time_remaining += f"{int(hours)}h "
                if minutes > 0:
                    time_remaining += f"{int(minutes)}m "
                time_remaining += f"{int(seconds)}s"
                await interaction.response.send_message(f"{interaction.user.mention}, you've already claimed your daily credits. Please try again in {time_remaining}.")
        except Exception as e:
            self.bot.logger.error(f"Error processing claim for user {user_id}: {e}", exc_info=True)
            await interaction.response.send_message("An error occurred while processing your claim. Please try again later.")

    # Error Handlers
    @add_credit.error
    @deduct_credit.error
    @balance.error
    @claim.error
    async def command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            await interaction.response.send_message("You do not have permission to use this command.")
        elif isinstance(error, app_commands.CommandInvokeError):
            await interaction.response.send_message("An unexpected error occurred while processing the command.")
            self.bot.logger.error(f"Error in command {interaction.command.name}: {error}")
        else:
            await interaction.response.send_message("An unexpected error occurred while processing the command.")
            self.bot.logger.error(f"Unhandled error in command {interaction.command.name}: {error}")

async def setup(bot):
    await bot.add_cog(CreditCommands(bot))