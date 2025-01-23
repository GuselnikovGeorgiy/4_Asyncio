import asyncio
from io import BytesIO

import aiohttp
from datetime import datetime, timedelta, date

import pandas as pd
from fake_useragent import UserAgent
from time import time

from database import create_tables, save
from model import SpimexTradingResults

BASE_URL = "https://spimex.com"


async def main() -> None:
    await create_tables()
    await get_tasks_reports()


async def get_tasks_reports() -> None:
    report_date = datetime.now().date()
    tasks = []
    async with aiohttp.ClientSession() as session:
        while report_date.year > 2023:
            date_str = report_date.strftime('%Y%m%d')
            url = f"{BASE_URL}/upload/reports/oil_xls/oil_xls_{date_str}162000.xls"
            tasks.append(process_report(session, url, report_date))
            report_date -= timedelta(days=1)
        await asyncio.gather(*tasks)


async def process_report(session: aiohttp.ClientSession, url: str, report_date: date) -> None:
    async with session.get(url, headers={"User-Agent": UserAgent().chrome}) as response:
        if response.status == 200:

            print(f"Downloaded report for {report_date}")

            data: bytes = await response.read()

            start_marker = "Единица измерения: Метрическая тонна"
            end_marker = "Итого:"

            with pd.ExcelFile(BytesIO(data)) as xls:
                excel_data = pd.read_excel(xls, header=None)
                try:
                    start_row = excel_data.loc[excel_data.eq(start_marker).any(axis=1)].index[0] + 3
                    end_row = excel_data.loc[excel_data.eq(end_marker).any(axis=1)].index[0]
                except IndexError:
                    raise Exception(f"Report for {report_date} not found")

            report_data = excel_data[start_row:end_row]
            report_data = report_data.drop(report_data.columns[0], axis=1)
            report_data = report_data[report_data.iloc[:, -1] != "-"]
            print(f"Processed report for {report_date}")

            await save_to_db(report_data, report_date)


async def save_to_db(report: pd.DataFrame, report_date: date) -> None:
    print(f"Saving to DB report for {report_date}")
    records = []
    try:
        for _, row in report.iterrows():
            records.append(SpimexTradingResults(
                exchange_product_id=row.iloc[0],
                exchange_product_name=row.iloc[1],
                oil_id=row.iloc[0][:4],
                delivery_basis_id=row.iloc[0][4:7],
                delivery_basis_name=row.iloc[2],
                delivery_type_id=row.iloc[0][-1],
                volume=int(row.iloc[3]),
                total=int(row.iloc[4]),
                count=int(row.iloc[-1]),
                date=report_date
            ))
    except Exception as e:
        print(e)
    await save(records)


if __name__ == "__main__":
    start = time()
    asyncio.run(main())
    print(f"Execute time: {time() - start}")
