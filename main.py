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


async def main() -> None:
    await create_tables()
    reports = await get_reports()
    print(len(reports), type(reports))
    for report in reports:
        if report is not None:
            await save_to_db(report[0], report[1])


async def get_reports():
    report_date = datetime.now().date()
    async with aiohttp.ClientSession() as session:
        tasks = []

        while report_date > datetime(2024, 1, 1).date():
            date_str = report_date.strftime('%Y%m%d')
            url = f"{BASE_URL}/upload/reports/oil_xls/oil_xls_{date_str}162000.xls"
            tasks.append(process_report(session, url, report_date))
            report_date -= timedelta(days=1)
        return await asyncio.gather(*tasks)


async def process_report(session: aiohttp.ClientSession, url: str, report_date: date) -> (pd.DataFrame, date):
    retries = 5     # Максимальное количество повторных попыток
    delay = 2       # Задержка между попытками

    for attempt in range(retries):
        try:
            async with session.get(url, headers={"User-Agent": UserAgent().random}) as response:
                if response.status == 200:

                    print(f"Downloaded report for {report_date}")

                    data: bytes = await response.read()

                    start_marker = "Единица измерения: Метрическая тонна"
                    end_marker = "Итого:"

                    excel_data = pd.read_excel(BytesIO(data), header=None)
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
                        raise Exception(f"Error with report {report_date}")

                    print(f"Processed report for {report_date}")

                    return excel_data, report_date

        except (aiohttp.ClientError, Exception) as e:
            print(f"Attempt {attempt + 1} failed for {report_date}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                print(f"Giving up on report for {report_date} after {retries} attempts")


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


if __name__ == "__main__":
    start = time()
    asyncio.run(main())
    print(f"Execute time: {time() - start}")
