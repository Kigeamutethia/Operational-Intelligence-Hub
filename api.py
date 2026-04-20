import requests

HF_TOKEN = "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"

MODEL_URL = "https://api-inference.huggingface.co/models/microsoft/Phi-3-mini-4k-instruct"


def call_phi3(prompt):
    headers = {
        "Authorization": f"Bearer {HF_TOKEN}"
    }

    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": 400,
            "temperature": 0.3,
            "return_full_text": False
        }
    }

    response = requests.post(MODEL_URL, headers=headers, json=payload)

    return response.json()