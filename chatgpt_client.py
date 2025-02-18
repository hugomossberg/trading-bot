import openai
import os 
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CHATGPT_API")
def chat_gpt(user_message):
    try:
        client = openai.OpenAI(api_key=API_KEY)  # Ange API-nyckeln här

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Jag är en noggran ai som gillar detaljer"},
                {"role": "user", "content": user_message}
            ]
        )
        return response.choices[0].message.content  # Returnera GPT-svaret
    
    except openai.OpenAIError as e:
        print(f"OpenAI API-fel: {e}")
        return "Ett fel uppstod vid anropet till OpenAI."
    except Exception as e:
        print(f"Ett oväntat fel uppstod: {e}")
        return "Ett oväntat fel inträffade."