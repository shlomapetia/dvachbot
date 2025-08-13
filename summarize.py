import aiohttp

async def summarize_text_with_hf(prompt: str, text: str, hf_token: str) -> str:
    # Адаптивные параметры длины summary
    max_len = min(350, max(100, len(text)//20))
    min_len = min(60, max(30, len(text)//40))

    url = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
    headers = {"Authorization": f"Bearer {hf_token}"}
    payload = {
        "inputs": f"{prompt}\n\n{text}",
        "parameters": {"min_length": min_len, "max_length": max_len}
    }
    print(f"[summarize] payload params: min={min_len}, max={max_len}, textlen={len(text)}")  # Для дебага

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=120) as resp:
                error = await resp.text()
                if resp.status != 200:
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
