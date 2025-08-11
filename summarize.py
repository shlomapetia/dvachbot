import aiohttp

async def summarize_text_with_hf(prompt: str, text: str, hf_token: str) -> str:
    url = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
    headers = {"Authorization": f"Bearer {hf_token}"}
    payload = {
        "inputs": f"{prompt}\n\n{text}",
        "parameters": {"min_length": 100, "max_length": 700}
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=120) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    print(f"[summarize] HF API error: {error}")
                    return ""
                data = await resp.json()
                if isinstance(data, list) and 'summary_text' in data[0]:
                    print("[summarize] HF API summary success")
                    return data[0]['summary_text']
                print(f"[summarize] HF API response format error: {data}")
                return ""
    except Exception as e:
        print(f"[summarize] Exception in summarize_text_with_hf: {e}")
        return ""
