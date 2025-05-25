import asyncio
import aiohttp
import pandas as pd
import random
from fake_useragent import UserAgent
from datetime import datetime

config_values = {}
with open('./config.txt', 'r') as file:
    for line in file:
        line = line.strip()
        if '=' in line:
            key, value = line.split('=', 1)
    
            if key == 'batch_size':
                config_values['batch_size'] = int(value)
            elif key == 'concurrency':
                config_values['concurrency'] = int(value)
            elif key == 'retries':
                config_values['retries'] = int(value)
            elif key =='license':
                config_values['license'] = str(value)

print(config_values)

now = datetime.now()
timestamp = now.strftime("%Y-%m-%d %H-%M-%S")

batch_size = config_values.get('batch_size')
concurrency = config_values.get('concurrency')
retries = config_values.get('retries')


async def fetch(session, batch):
    url = "https://phone.securenet.fun/api/v1/device/getvalidate"
    headers = {
        "Content-Type": "application/json",
    }
    payload = {
        "inputNum": batch,
        "license": config_values.get('license')  
    }

    async with session.post(url, json=payload, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data.get("results", [])


async def bound_fetch(sem, session, batch):
    async with sem:
        try:
            print(f"Processing batch: {batch}")
            results = await fetch(session, batch)
            if not results:
                print(f"⚠️ No results for batch: {batch}")
            await asyncio.sleep(random.uniform(1, 2)) 
            return results or []
        except Exception as e:
            print(f"❌ Error on batch {batch}: Reprocessing")
            return []

def parse_results(results):
    output_rows = []
    for result in results:
        telnyx_data = result.get("telnyxData")
        telnyx = telnyx_data.get("data") if telnyx_data else {}

        carrier = telnyx.get("carrier", {}) if telnyx else {}
        caller_name = telnyx.get("caller_name", {}) if telnyx else {}
        portability = telnyx.get("portability", {}) if telnyx else {}

        isValid = 'FALSE' if (
                    (caller_name and 'fraud' in (caller_name.get("caller_name") or "").lower()) or
                    ((caller_name is None or caller_name.get("caller_name") in [None, '', 'null']) and
                    (portability is None or portability.get("city") in [None, '', 'null']))
                ) else 'TRUE'



        output_rows.append({
            "original": result.get("original") if result else None,
            "isValid": isValid,
            "formattedNumber": result.get("formattedNumber") if result else None,
            "numberType": result.get("numberType") if result else None,
            "regionCode": result.get("regionCode") if result else None,
            "country_code": telnyx.get("country_code") if telnyx else None,
            "carrier_name": carrier.get("name") if carrier else None,
            "line_type": carrier.get("type") if carrier else None,
            "caller_name": caller_name.get("caller_name") if caller_name else None,
            "city": portability.get("city") if portability else None,
            "state": portability.get("state") if portability else None,
        })

    return output_rows

async def main():
    input_file = "../input_numbers.xlsx"
    output_file = f"../output_results-{timestamp}.xlsx"

    df = pd.read_excel(input_file)
    numbers = df.iloc[:, 0].dropna().astype(str).tolist()
    numbers = list(dict.fromkeys(numbers)) 

    batches = [numbers[i:i + batch_size] for i in range(0, len(numbers), batch_size)]
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        current_batches = batches
        all_results = []

        
        for attempt in range(1, retries + 1):
            print(f"\n🔁 Attempt {attempt} for {len(current_batches)} batches")
            tasks = [bound_fetch(sem, session, batch) for batch in current_batches]
            batch_results = await asyncio.gather(*tasks)

            for res in batch_results:
                if res:
                    all_results.extend(res)

            failed_batches = [batch for batch, res in zip(current_batches, batch_results) if not res]

            if not failed_batches:
                print("✅ All batches succeeded.")
                break
            else:
                print(f"⚠️ {len(failed_batches)} batches failed, retrying...")
                current_batches = failed_batches

       
        incomplete = [r for r in all_results if not r.get("telnyxData") or not r["telnyxData"].get("data")]
        complete = [r for r in all_results if r.get("telnyxData") and r["telnyxData"].get("data")]

        if incomplete:
            print(f"\n🔍 Retrying {len(incomplete)} incomplete records individually...")

            retry_batches = [[r["original"]] for r in incomplete if r.get("original")]
            retry_tasks = [bound_fetch(asyncio.Semaphore(1), session, batch) for batch in retry_batches]
            retry_results = await asyncio.gather(*retry_tasks)

           
            retried_flat = [item for sub in retry_results for item in sub if item]
            all_results = complete + retried_flat
        else:
            print("\n✅ No incomplete results found.")

        output_rows = parse_results(all_results)
        output_df = pd.DataFrame(output_rows)
        output_df.to_excel(output_file, index=False)
        print(f"\n📁 Saved {len(output_rows)} records to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
