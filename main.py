import asyncio
from io import BytesIO

import aiohttp
from datetime import datetime, timedelta, date

import pandas as pd
from fake_useragent import UserAgent
from time import time

from database import create_tables, async_session_maker
from model import SpimexTradingResults

BASE_URL = "https://spimex.com"


def get_urls(year: int = 2025, month: int = 1, day: int = 1) -> list[tuple[str, date]]:
    report_date = datetime.now().date()
    urls = []
    while report_date > datetime(year, month, day).date():
        urls += [
            (f"{BASE_URL}/upload/reports/oil_xls/oil_xls_{report_date.strftime('%Y%m%d')}162000.xls", report_date)
        ]
        report_date -= timedelta(days=1)
    return urls


async def get_reports(urls: list[tuple[str, date]]):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url, report_date in urls:
            tasks.append(download_report(session, url, report_date))
        return await asyncio.gather(*tasks)


async def download_report(session: aiohttp.ClientSession, url: str, report_date: date) -> (bytes, date):
    retries = 5  # Максимальное количество повторных попыток
    delay = 2  # Задержка между попытками

    for attempt in range(retries):
        try:
            async with session.get(url, headers={"User-Agent": UserAgent().random}) as response:
                if response.status == 200:
                    print(f"Downloaded report for {report_date}")
                    return await response.read(), report_date
                else:
                    return None
        except (aiohttp.ClientError, Exception) as e:
            print(f"Attempt {attempt + 1} failed for {report_date}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                print(f"Giving up on report for {report_date} after {retries} attempts")


def process_reports(reports: list[tuple[bytes, date]]) -> list[tuple[pd.DataFrame, date]]:
    processed_reports = []
    for report in reports:
        if report is not None:

            start_marker = "Единица измерения: Метрическая тонна"
            end_marker = "Итого:"

            excel_data = pd.read_excel(BytesIO(report[0]), header=None)
            try:
                excel_data = excel_data.drop(excel_data.columns[0], axis=1)
                start_row = excel_data[excel_data.eq(start_marker).any(axis=1)].index[0] + 3
                end_rows = excel_data[excel_data.eq(end_marker).any(axis=1)].index
                end_row = end_rows[end_rows > start_row][0]
                excel_data = excel_data[start_row:end_row]
                excel_data = excel_data[excel_data.iloc[:, -1] != "-"]
                if excel_data.empty:
                    raise
            except IndexError:
                raise Exception(f"Error with report {report[1]}")

            print(f"Processed report for {report[1]}")

            processed_reports.append((excel_data, report[1]))
    return processed_reports


async def save_to_db(report: pd.DataFrame, report_date: date) -> None:
    print(f"Saving to DB report for {report_date}")
    if report.empty:
        return
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

    async with async_session_maker() as session:
        try:
            async with session.begin():
                session.add_all(records)
            await session.commit()
        except Exception as e:
            print(e)
            await session.rollback()


async def main() -> None:
    start = time()
    await create_tables()
    urls = get_urls(year=2025, month=1, day=1)
    reports = await get_reports(urls)
    processed_reports = process_reports(reports)
    for report in processed_reports:
        await save_to_db(report[0], report[1])
    print(f"Execute time: {time() - start}")


if __name__ == "__main__":
    asyncio.run(main())
