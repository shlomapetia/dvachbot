import aiohttp
import json

async def summarize_text_with_hf(prompt: str, text: str, hf_token: str) -> str:
    # Используем новую, более современную и подходящую для русского языка модель
    url = "https://api-inference.huggingface.co/models/cointegrated/rut5-base-absum"
    headers = {"Authorization": f"Bearer {hf_token}"}

    # Адаптивные параметры длины. Модели T5 хорошо работают с соотношениями.
    # Устанавливаем минимальную длину в 10% от оригинала, но не менее 40 слов.
    # Максимальную - в 30%, но не более 400 слов.
    min_len = max(100, len(text.split()) // 10)
    max_len = min(600, len(text.split()) // 3)

    # Модели T5 часто используют немного другие параметры в payload
    payload = {
        "inputs": f"summarize: {prompt}\n\n{text}", # Добавляем префикс "summarize: ", типичный для T5
        "parameters": {
            "min_length": min_len,
            "max_length": max_len,
            "no_repeat_ngram_size": 3, # Уменьшаем повторение фраз
            "early_stopping": True,    # Модель может остановиться, когда сочтет summary готовым
        },
        "options": {
            "wait_for_model": True # Ждать, если модель загружается
        }
    }
    print(f"[summarize] payload params: min={min_len}, max={max_len}, textlen={len(text)}")

    try:
        async with aiohttp.ClientSession() as session:
            # Увеличим таймаут, т.к. большие модели могут дольше обрабатывать запрос
            async with session.post(url, headers=headers, json=payload, timeout=180) as resp:
                response_text = await resp.text()
                
                if resp.status != 200:
                    print(f"[summarize] HF API error. Status: {resp.status}, Response: {response_text}")
                    return ""
                    
                try:
                    data = json.loads(response_text)
                    if isinstance(data, list) and data and 'summary_text' in data[0]:
                        print("[summarize] HF API summary success")
                        return data[0]['summary_text']
                    
                    # Логируем неожиданный, но валидный JSON-ответ
                    print(f"[summarize] HF API response format error: {data}")
                    return ""
                except json.JSONDecodeError:
                    # Логируем случай, когда ответ не является валидным JSON
                    print(f"[summarize] HF API JSON decode error. Response: {response_text}")
                    return ""

    except Exception as e:
        import traceback
        print(f"[summarize] Exception in summarize_text_with_hf: {e}")
        traceback.print_exc()
        return ""
