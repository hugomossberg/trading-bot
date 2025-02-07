import openai

client = openai.OpenAI(api_key="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")

models = client.models.list()
for model in models:
    print(model.id)
