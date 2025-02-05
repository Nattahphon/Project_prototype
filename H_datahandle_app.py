# import os
# import pandas as pd
# import logging

# Set up logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # # # for no gui 
# class DataHandler:
#     _instance = None

#     def __new__(cls, *args, **kwargs):
#         if not cls._instance:
#             cls._instance = super(DataHandler, cls).__new__(cls)
#         return cls._instance

#     def __init__(self, dataset_paths=None):
#         """
#         Initialize the DataHandler with dataset paths.
        
#         :param dataset_paths: A dictionary where keys are identifiers (e.g., "df1", "df2")
#                               and values are file paths to the datasets.
#         """
#         if not hasattr(self, '_initialized'):  
#             self._initialized = True
#             if dataset_paths is None:
#                 dataset_paths = {}
#             self.dataset_paths = dataset_paths
#             self._data = {}

#     def load_data(self) -> None:
#         """Load and standardize data from all provided file paths."""
#         if not self.dataset_paths:
#             raise ValueError("No dataset paths provided.")

#         for key, dataset_path in self.dataset_paths.items():
#             if not os.path.exists(dataset_path):
#                 raise FileNotFoundError(f"Dataset file not found at {dataset_path}.")

#             _, ext = os.path.splitext(dataset_path)
#             if ext == ".csv":
#                 try:
#                     self._data[key] = pd.read_csv(dataset_path, encoding="utf-8")
#                 except UnicodeDecodeError:
#                     logging.warning(f"UTF-8 decoding failed for {key}. Trying 'latin1'.")
#                     self._data[key] = pd.read_csv(dataset_path, encoding="latin1")
#             elif ext in [".xls", ".xlsx"]:
#                 self._data[key] = pd.read_excel(dataset_path)
#             else:
#                 raise ValueError(f"Unsupported file extension for {key}: {ext}")

#             # Standardize column names
#             self._data[key].columns = (
#                 self._data[key].columns.str.lower().str.strip().str.replace(" ", "_")
#             )
#             logging.info(f"Data for {key} loaded. Columns: {', '.join(self._data[key].columns)}")

#     def preprocess_data(self) -> None:
#         """Preprocess numeric-like columns for all datasets."""
#         if not self._data:
#             raise ValueError("Data not loaded.")

#         for key, df in self._data.items():
#             numeric_like_cols = [
#                 col
#                 for col in df.columns
#                 if df[col].dtype == "object" and df[col].str.contains(r"[\d,.$€¥-]").any()
#             ]

#             for col in numeric_like_cols:
#                 try:
#                     df[col] = df[col].str.replace(r"[^\d.-]", "", regex=True)
#                     df[col] = pd.to_numeric(df[col], errors="coerce")
#                 except Exception as e:
#                     logging.error(f"Error processing column {col} in dataset {key}: {e}")

#         logging.info("Preprocessing complete.")

#     def get_data(self, key: str) -> pd.DataFrame:
#         """
#         Retrieve the loaded data for a specific key.

#         :param key: The key identifying the dataset (e.g., "df1", "df2").
#         :return: The loaded DataFrame.
#         """
#         if key not in self._data:
#             raise ValueError(f"Data for key '{key}' not loaded.")
#         return self._data[key]

# for streamlit
# Set up logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# class DataHandler:
#     _instance = None

#     def __new__(cls, *args, **kwargs):
#         if not cls._instance:
#             cls._instance = super(DataHandler, cls).__new__(cls)
#         return cls._instance

#     def __init__(self, dataset_paths=None):
#         """
#         Initialize the DataHandler with dataset paths.
#         :param dataset_paths: A dictionary where keys are identifiers (e.g., "df1", "df2")
#                               and values are file paths to the datasets.
#         """
#         if not hasattr(self, '_initialized'):  
#             self._initialized = True
#             if dataset_paths is None:
#                 dataset_paths = {}
#             self.dataset_paths = dataset_paths
#             self._data = {}

#     def load_data(self) -> None:
#         """Load and standardize data from all provided file paths."""
#         if not self.dataset_paths:
#             raise ValueError("No dataset paths provided.")

#         for key, dataset_path in self.dataset_paths.items():
#             if not os.path.exists(dataset_path):
#                 raise FileNotFoundError(f"Dataset file not found at {dataset_path}.")

#             _, ext = os.path.splitext(dataset_path)
#             if ext == ".csv":
#                 try:
#                     self._data[key] = pd.read_csv(dataset_path, encoding="utf-8")
#                 except UnicodeDecodeError:
#                     logging.warning(f"UTF-8 decoding failed for {key}. Trying 'latin1'.")
#                     self._data[key] = pd.read_csv(dataset_path, encoding="latin1")
#             elif ext in [".xls", ".xlsx"]:
#                 self._data[key] = pd.read_excel(dataset_path)
#             else:
#                 raise ValueError(f"Unsupported file extension for {key}: {ext}")

#             # Standardize column names
#             self._data[key].columns = (
#                 self._data[key].columns.str.lower().str.strip().str.replace(" ", "_")
#             )
#             logging.info(f"Data for {key} loaded. Columns: {', '.join(self._data[key].columns)}")

#     def preprocess_data(self) -> None:
#         """Preprocess numeric-like columns for all datasets."""
#         if not self._data:
#             raise ValueError("Data not loaded.")

#         for key, df in self._data.items():
#             numeric_like_cols = [
#                 col
#                 for col in df.columns
#                 if df[col].dtype == "object" and df[col].str.contains(r"[\d,.$€¥-]").any()
#             ]

#             for col in numeric_like_cols:
#                 try:
#                     df[col] = df[col].str.replace(r"[^\d.-]", "", regex=True)
#                     df[col] = pd.to_numeric(df[col], errors="coerce")
#                 except Exception as e:
#                     logging.error(f"Error processing column {col} in dataset {key}: {e}")

#         logging.info("Preprocessing complete.")

#     def get_data(self, key: str) -> pd.DataFrame:
#         """
#         Retrieve the loaded data for a specific key.
#         :param key: The key identifying the dataset (e.g., "df1", "df2").
#         :return: The loaded DataFrame.
#         """
#         if key not in self._data:
#             raise ValueError(f"Data for key '{key}' not loaded.")
#         return self._data[key]


import os
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataHandler:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DataHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, dataset_paths=None):
        """
        Initialize the DataHandler with dataset paths.
        :param dataset_paths: A dictionary where keys are identifiers (e.g., "df1", "df2")
                              and values are file paths to the datasets.
        """
        if not hasattr(self, '_initialized'):
            self._initialized = True
            if dataset_paths is None:
                dataset_paths = {}
            self.dataset_paths = dataset_paths
            self._data = {}

    def load_data(self) -> None:
        """Load and standardize data from all provided file paths."""
        if not self.dataset_paths:
            raise ValueError("No dataset paths provided.")

        for key, dataset_path in self.dataset_paths.items():
            if not os.path.exists(dataset_path):
                raise FileNotFoundError(f"Dataset file not found at {dataset_path}.")

            _, ext = os.path.splitext(dataset_path)
            if ext == ".csv":
                try:
                    self._data[key] = pd.read_csv(dataset_path, encoding="utf-8")
                except UnicodeDecodeError:
                    logging.warning(f"UTF-8 decoding failed for {key}. Trying 'latin1'.")
                    self._data[key] = pd.read_csv(dataset_path, encoding="latin1")
            elif ext in [".xls", ".xlsx"]:
                self._data[key] = pd.read_excel(dataset_path)
            else:
                raise ValueError(f"Unsupported file extension for {key}: {ext}")

            # Standardize column names
            self._data[key].columns = (
                self._data[key].columns.str.lower().str.strip().str.replace(" ", "_")
            )
            logging.info(f"Data for {key} loaded. Columns: {', '.join(self._data[key].columns)}")

    def preprocess_data(self, threshold: float = 0.8) -> None:
        """
        Preprocess numeric-like and datetime-like columns for all datasets.
        
        สำหรับแต่ละคอลัมน์ที่เป็น object:
          1. หากคอลัมน์มีตัวเลข เราจะลองแปลงให้เป็น numeric
          2. หากคอลัมน์มีลักษณะของวันที่ (เช่น มีรูปแบบ YYYY-MM-DD หรือ YYYY/MM/DD)
             เราจะลองแปลงให้เป็น datetime
             
        หากอัตราส่วนของค่าที่แปลงได้ (non-NaN) สูงกว่า threshold (ค่าเริ่มต้น 0.8)
        จะถือว่าคอลัมน์นั้นควรแปลงตามประเภทที่ตรวจพบ
        """
        if not self._data:
            raise ValueError("Data not loaded.")

        # Regular expression สำหรับตรวจสอบรูปแบบวันที่
        date_pattern = r"\d{4}[-/]\d{1,2}[-/]\d{1,2}"

        for key, df in self._data.items():
            for col in df.columns:
                # ตรวจสอบเฉพาะคอลัมน์ที่เป็น object
                if df[col].dtype != "object":
                    continue

                # ตรวจสอบว่าคอลัมน์มีตัวเลขหรือไม่
                if not df[col].str.contains(r"\d", na=False).any():
                    continue

                # 1. ลองแปลงเป็น datetime หากพบลักษณะวันที่
                if df[col].str.contains(date_pattern, na=False).any():
                    try:
                        datetime_series = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                    except Exception as e:
                        logging.error(f"Error converting column '{col}' in dataset '{key}' to datetime: {e}")
                        datetime_series = pd.Series([pd.NaT] * len(df[col]))
                        
                    non_na_ratio = datetime_series.notna().mean()
                    logging.info(f"Column '{col}' in dataset '{key}' datetime conversion ratio: {non_na_ratio:.2f}")

                    if non_na_ratio >= threshold:
                        df[col] = datetime_series
                        logging.info(f"Column '{col}' in dataset '{key}' converted to datetime.")
                        continue  # หากแปลงเป็น datetimeสำเร็จแล้ว ให้ข้ามไปขั้นตอนต่อไปของคอลัมน์นี้

                # 2. หากไม่ใช่ datetime ให้ลองแปลงเป็น numeric
                # ทำความสะอาดข้อมูลในคอลัมน์ โดยลบอักขระที่ไม่ใช่ตัวเลข จุด หรือเครื่องหมายลบ
                cleaned = df[col].str.replace(r"[^\d.-]", "", regex=True)
                numeric_series = pd.to_numeric(cleaned, errors="coerce")
                non_na_ratio = numeric_series.notna().mean()
                logging.info(f"Column '{col}' in dataset '{key}' numeric conversion ratio: {non_na_ratio:.2f}")

                if non_na_ratio >= threshold:
                    df[col] = numeric_series
                    logging.info(f"Column '{col}' in dataset '{key}' converted to numeric.")
                else:
                    logging.info(f"Column '{col}' in dataset '{key}' skipped conversion (ratio below threshold).")

        logging.info("Preprocessing complete.")

    def get_data(self, key: str) -> pd.DataFrame:
        """
        Retrieve the loaded data for a specific key.
        :param key: The key identifying the dataset (e.g., "df1", "df2").
        :return: The loaded DataFrame.
        """
        if key not in self._data:
            raise ValueError(f"Data for key '{key}' not loaded.")
        return self._data[key]

