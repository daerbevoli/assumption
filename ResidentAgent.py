import pandas as pd

class ResidentAgent:
    def __init__(self, data=None):
        """
        Initializes the ResidentAgent with a private pandas DataFrame.
        :param data: Optional initial data for the DataFrame.
        """
        if data is None:
            data = pd.DataFrame()
        elif not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame")

        self._load = data

    def get_load(self):
        """Returns a copy of the private DataFrame."""
        return self._load.copy()

    def set_load(self, new_data):
        """Sets the private DataFrame with a new DataFrame."""
        if isinstance(new_data, pd.DataFrame):
            self._load = new_data
        else:
            raise ValueError("Data must be a pandas DataFrame")

    def add_data(self, new_row):
        """Appends a new row (dict or Series) to the DataFrame."""
        if isinstance(new_row, dict) or isinstance(new_row, pd.Series):
            self._load = pd.concat([self._load, pd.DataFrame([new_row])], ignore_index=True)
        else:
            raise ValueError("New row must be a dictionary or pandas Series")

    def __repr__(self):
        return f"ResidentAgent(load=\n{self._load})"


