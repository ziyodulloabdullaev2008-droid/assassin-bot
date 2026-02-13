#!/usr/bin/env python3
import sys

with open('bot.py', 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

# Fix lines 996-997 (0-indexed: 995-996)
# The issue is in the info string building in cmd_broadcast_menu function
lines[996] = '    info += f"📝 <b>Формат:</b> {config.get(\'parse_mode\', \'HTML\')}\\n"\n'
lines[997] = '    info += f"💭 <b>Чатов:</b> {len(chats)}\\n"\n'

with open('bot.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("✅ Fixed corrupted emoji characters in lines 996-997")

# Verify the fix
with open('bot.py', 'r', encoding='utf-8') as f:
    verify_lines = f.readlines()
    print("\nVerified:")
    print(f"996: {repr(verify_lines[996].strip())}")
    print(f"997: {repr(verify_lines[997].strip())}")
