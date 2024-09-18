import discord
from discord import Embed, Colour
from typing import Optional

class EmbedCreator:
    def __init__(self, default_color: Colour = Colour.blue()):
        self.default_color = default_color

    def create_embed(self, title: str, description: Optional[str] = None, color: Optional[Colour] = None, 
                     image_url: Optional[str] = None, footer_text: Optional[str] = None, 
                     fields: Optional[list] = None) -> Embed:
        embed = Embed(title=title, description=description, color=color or self.default_color)

        if image_url:
            embed.set_image(url=image_url)

        if footer_text:
            embed.set_footer(text=footer_text)

        if fields:
            for name, value, inline in fields:
                embed.add_field(name=name, value=value, inline=inline)

        return embed

    def create_image_embed(self, image_url: str, title: str, description: Optional[str] = None) -> Embed:
        return self.create_embed(title=title, description=description, image_url=image_url)

    def create_confirmation_embed(self, title: str, description: str) -> Embed:
        return self.create_embed(title=title, description=description, color=Colour.green())

    def create_error_embed(self, title: str, description: str) -> Embed:
        return self.create_embed(title=title, description=description, color=Colour.red())

    def create_info_embed(self, title: str, description: str, footer_text: Optional[str] = None, image_url: Optional[str] = None) -> Embed:
        return self.create_embed(title=title, description=description, footer_text=footer_text, image_url=image_url)

    def create_product_embed(self, title: str, description: str, image_url: str, price: str, vendor: str) -> Embed:
        fields = [
            ("Price", price, True),
            ("Vendor", vendor, True)
        ]
        return self.create_embed(title=title, description=description, image_url=image_url, fields=fields)

    def create_warning_embed(self, title: str, description: str) -> Embed:
        return self.create_embed(title=title, description=description, color=Colour.orange())

    def create_action_embed(self, title: str, description: str, action_text: str, action_url: str) -> Embed:
        fields = [("Action", f"[{action_text}]({action_url})", False)]
        return self.create_embed(title=title, description=description, fields=fields)