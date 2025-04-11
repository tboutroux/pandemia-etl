import pandas as pd

def extract_data(path: str) -> pd.DataFrame:
    """
    Load data from a CSV file.

    Args:
        path (str): The path to a CSV/JSON file.

    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    
    try:
        if path.endswith('.csv'):
            data = pd.read_csv(path)
        elif path.endswith('.json'):
            data = pd.read_json(path)
        else:
            raise ValueError("Unsupported file format. Please provide a CSV or JSON file.")
        
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()
