import pandas as pd
import os

csv_path = r"D:\lakehouse_demo\duckdb\data\customers-2000000.csv"
df = pd.read_csv(csv_path)

target_size = 2 * 1024**3

current_size = os.path.getsize(csv_path)
print(f"Orijinal CSV boyutu: {current_size/1024**2:.2f} MB")

repeat_factor = int(target_size // current_size) + 1
print(f"Tekrarlama katsayısı: {repeat_factor}")

big_df = pd.concat([df] * 30, ignore_index=True)

big_df.to_parquet("D:\lakehouse_demo\duckdb\data\customers-2gb.parquet", engine="pyarrow", index=False)
print("customers-2gb.parquet oluşturuldu ✅")
