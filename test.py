import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_chat_id():
    # Get bot token from environment variable
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not bot_token:
        print("Please set your TELEGRAM_BOT_TOKEN in the .env file")
        return

    # Make request to Telegram API
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"

    try:
        response = requests.get(url)
        data = response.json()

        if not data.get("ok"):
            print("Error getting updates. Make sure your bot token is correct.")
            return

        if not data["result"]:
            print("\nNo recent chat history found. Please:")
            print("1. Start a chat with your bot")
            print("2. Send a message to your bot")
            print("3. Run this script again")
            return

        # Get the most recent chat ID
        chat_id = data["result"][-1]["message"]["chat"]["id"]
        chat_type = data["result"][-1]["message"]["chat"]["type"]
        chat_title = data["result"][-1]["message"]["chat"].get("title", "Private Chat")

        print(f"\nFound Chat ID: {chat_id}")
        print(f"Chat Type: {chat_type}")
        print(f"Chat Title: {chat_title}")
        print("\nAdd this to your .env file:")
        print(f"TELEGRAM_CHAT_ID={chat_id}")

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    get_chat_id()
