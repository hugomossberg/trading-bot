import openai
import os 

API_KEY = "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"

def chat_gpt(user_message):
    try:
        client = openai.OpenAI(api_key=API_KEY)  # Ange API-nyckeln här

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Du är en hjälpsam AI-assistent."},
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