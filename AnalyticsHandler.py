# Math and Pandas
import numpy as np
import pandas as pd 

# Visualization Stuff
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns


class AnalyticsHandler:
    """
    This class handles various data analysis tasks for a provided DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to be analyzed.
    """
    def __init__(self, df):
        self.data = df

    def triangle_correlation_heatmap(self):
        """
        Creates a correlation matrix and plots a triangular correlation heatmap.
        """
        # Create correlation matrix
        corr = self.data.corr()

        plt.figure(figsize=(30, 20))
        # Define the mask to set the values in the upper triangle to True
        mask = np.triu(np.ones_like(corr, dtype= bool))
        heatmap = sns.heatmap(corr, mask=mask, vmin=-1, vmax=1, annot=True, cmap='BrBG')
        heatmap.set_title('Triangle Correlation Heatmap', fontdict={'fontsize':18}, pad=16)

    def analyze_correlations(self, threshold, plot=False, trendline=False):
        """
        Calculates the correlation matrix and finds pairs of variables with correlation exceeding the provided threshold.
        Optionally plots scatter plots of pairs with high correlation.

        Args:
            threshold (float): The correlation threshold.
            plot (bool, optional): Whether to plot scatter plots of the pairs with high correlation. Default is False.
            trendline (bool, optional): Whether to plot a trend line on the scatter plot. Default is False.
        """
        # Calculate the correlation matrix
        correlation_matrix = self.data.corr()

        # Find pairs of variables with correlation exceeding the threshold
        strong_correlations = dict()

        for column in correlation_matrix:
            for row in correlation_matrix.index:
                if column != row:  # Avoid self-correlation
                    correlation = correlation_matrix.loc[row, column]
                    if np.abs(correlation) > threshold:  # Consider absolute correlation values
                        pair = frozenset((row, column))  # Use frozenset to avoid duplicate pairs
                        if pair not in strong_correlations:
                            strong_correlations[pair] = correlation

        # Sort correlations in descending order
        sorted_correlations = sorted(strong_correlations.items(), key=lambda item: np.abs(item[1]), reverse=True)

        # Output strong correlations
        max_length = 20  # Maximum length for variable names
        for pair, correlation in sorted_correlations:
            # Shorten variable names if they are too long
            variable_names = [name[:max_length] for name in pair]
            print(f"Correlation between {variable_names[0]} and {variable_names[1]}:".ljust(60) + f"{correlation}")
            
            if plot:
                # Create a scatter plot for the pair
                x = list(pair)[0]
                y = list(pair)[1]

                # Convert the timestamps to numerical values if either variable is a datetime
                if np.issubdtype(self.data[x].dtype, np.datetime64):
                    x_values = mdates.date2num(self.data[x])
                else:
                    x_values = self.data[x]

                if np.issubdtype(self.data[y].dtype, np.datetime64):
                    y_values = mdates.date2num(self.data[y])
                else:
                    y_values = self.data[y]

                plt.scatter(x_values, y_values)

                if trendline:
                    # Perform the polynomial fit and plot the trendline
                    coefficients = np.polyfit(x_values, y_values, 1)
                    polynomial = np.poly1d(coefficients)
                    xs = np.linspace(min(x_values), max(x_values), 100)
                    plt.plot(xs, polynomial(xs), color='red')

                plt.title(f'Correlation between {x} and {y}')
                plt.xlabel(x)
                plt.ylabel(y)
                plt.show()

    def scatter_matrix(self, threshold=None):
        """
        Creates and displays a scatter matrix of the DataFrame.

        Args:
            threshold (float, optional): The correlation threshold for plotting variable pairs.
        """

        # Calculate the correlation matrix if a threshold is specified
        if threshold is not None:
            correlation_matrix = self.data.corr().abs()

            # Create a boolean mask for the pairs of variables with correlation above the threshold
            mask = (correlation_matrix > threshold) & (correlation_matrix != 1)

            # Create a list of variable pairs with correlation above the threshold
            variables_to_plot = [column for column in correlation_matrix.columns if mask[column].any()]
        else:
            variables_to_plot = self.data.columns

        # Create the scatter matrix for the selected variables
        sm = pd.plotting.scatter_matrix(self.data[variables_to_plot], figsize=(30, 30), diagonal='kde')

        for ax in sm.ravel():
            ax.set_xlabel(ax.get_xlabel(), fontsize = 20, rotation = 45)
            ax.set_ylabel(ax.get_ylabel(), fontsize = 20, rotation = 0)

        # May need to offset label when rotating to prevent overlap of figure
        [s.get_yaxis().set_label_coords(-0.5,0.5) for s in sm.reshape(-1)]

        # Hide all ticks
        [s.set_xticks(()) for s in sm.reshape(-1)]
        [s.set_yticks(()) for s in sm.reshape(-1)]
        plt.show()

    def plot_line_chart(self, variables, dual_axis=False):
        """Plots a line chart with variables from the DataFrame.

        Args:
            variables (list of str): A list containing the names of variables to plot.
            dual_axis (bool): If True and only two variables are provided, a dual y-axis is created.

        """

        # Check if the provided variables exist in the DataFrame
        for var in variables:
            if var not in self.data.columns:
                raise ValueError(f"The variable '{var}' does not exist in the DataFrame.")

        # Check if 'timestamp' is in the DataFrame
        if 'timestamp' not in self.data.columns:
            raise ValueError("The DataFrame does not contain a 'timestamp' column.")

        # Sort data by 'timestamp'
        sorted_data = self.data.sort_values('timestamp')

        # Create the plot
        fig, ax1 = plt.subplots(figsize=(10, 6))

        # Add each variable to the plot
        for i, var in enumerate(variables):
            if dual_axis and len(variables) == 2 and i == 1:  # create a second axis for the second variable
                ax2 = ax1.twinx()
                ax2.plot(sorted_data['timestamp'], sorted_data[var], label=var, color='tab:orange')
                ax2.set_ylabel(var, color='tab:orange')
            else:
                ax1.plot(sorted_data['timestamp'], sorted_data[var], label=var)

        # Add title and labels
        ax1.set_title("Line chart")
        ax1.set_xlabel("Time")
        ax1.set_ylabel(variables[0])

        # Add a legend
        fig.legend()

        # Display the plot
        plt.show()
    
    def plot_scatter(self, var1, var2, trendline=False):
        """Plots a scatter plot of the provided variables from the DataFrame and, optionally, a trend line.

        Args:
            var1 (str): The name of the variable to plot on the x-axis.
            var2 (str): The name of the variable to plot on the y-axis.
            trendline (bool, optional): Whether to plot a trend line. Default is False.
        """
        # Check if the provided variables exist in the DataFrame
        if var1 not in self.data.columns:
            raise ValueError(f"The variable '{var1}' does not exist in the DataFrame.")
        if var2 not in self.data.columns:
            raise ValueError(f"The variable '{var2}' does not exist in the DataFrame.")

        # Create a scatter plot
        plt.scatter(self.data[var1], self.data[var2], label=f'{var1} vs {var2}')

        if trendline:
            # Convert the timestamps to numerical values if either variable is a datetime
            if np.issubdtype(self.data[var1].dtype, np.datetime64):
                x = mdates.date2num(self.data[var1])
            else:
                x = self.data[var1]

            if np.issubdtype(self.data[var2].dtype, np.datetime64):
                y = mdates.date2num(self.data[var2])
            else:
                y = self.data[var2]

            # Perform the polynomial fit and plot the trendline
            coefficients = np.polyfit(x, y, 1)
            polynomial = np.poly1d(coefficients)
            xs = np.linspace(min(x), max(x), 100)
            plt.plot(xs, polynomial(xs), color='red')

        # Add title and labels
        plt.title(f'Scatter plot: {var1} vs {var2}')
        plt.xlabel(var1)
        plt.ylabel(var2)

        # Display the plot
        plt.show()

    def invesigate_correlation(self, variable1, variable2, plot_trendline=False):
        """Plots a scatter and line chart with variables from the DataFrame.

        Args:
            variable1, variable2 (str): The names of variables to plot.
            plot_trendline (bool): Whether to plot the trendline in the scatter plot. Defaults to False.

        """

        # Check if the provided variables exist in the DataFrame
        for var in [variable1, variable2]:
            if var not in self.data.columns:
                raise ValueError(f"The variable '{var}' does not exist in the DataFrame.")

        # Check if 'timestamp' is in the DataFrame
        if 'timestamp' not in self.data.columns:
            raise ValueError("The DataFrame does not contain a 'timestamp' column.")

        # Sort data by 'timestamp'
        sorted_data = self.data.sort_values('timestamp')

        # Calculate and print the correlation coefficient
        corr_coef = sorted_data[variable1].corr(sorted_data[variable2])
        print(f"Correlation coefficient between {variable1} and {variable2}: {corr_coef}")

        # Create two subplots
        fig, ax = plt.subplots(2, 1, figsize=(10, 12))

        # Add the variables to the scatter plot (first subplot)
        ax[0].scatter(sorted_data[variable1], sorted_data[variable2], label=f'Scatter-{variable1} vs {variable2}')

        if plot_trendline:
            # Calculate trendline
            z = np.polyfit(sorted_data[variable1], sorted_data[variable2], 1)
            p = np.poly1d(z)
            ax[0].plot(sorted_data[variable1], p(sorted_data[variable1]), "r--", label='Trendline')

        # Add title and labels to the scatter plot
        ax[0].set_title("Scatter chart")
        ax[0].set_xlabel(variable1)
        ax[0].set_ylabel(variable2)

        # Add a legend to the scatter plot
        ax[0].legend()

        # Add the first variable to the line plot (second subplot)
        ax1 = ax[1].twinx()
        ax1.plot(sorted_data['timestamp'], sorted_data[variable1], 'g-', label=f'Line-{variable1}')

        # Add the second variable to the line plot (second subplot), with a secondary y-axis
        ax[1].plot(sorted_data['timestamp'], sorted_data[variable2], 'b-', label=f'Line-{variable2}')

        # Add title and labels to the line plot
        ax[1].set_title("Line chart")
        ax[1].set_xlabel("Time")
        ax[1].set_ylabel(variable2, color='b')
        ax1.set_ylabel(variable1, color='g')

        # Add a legend to the line plot
        ax1.legend(loc='upper left')
        ax[1].legend(loc='upper right')

        # Display the plots
        plt.tight_layout()
        plt.show()

    def find_anomalies(self, rolling_window='3H', threshold=0.2, variables=None):        
        """
        This function scans the DataFrame for anomalies in the correlation between a specific variable and
        all other variables. The anomalies are identified based on the deviation of the rolling correlation from the
        overall correlation.

        Args:
            rolling_window (str, optional): Defines the length of the rolling window. By default, it's '3H' for 3 hours.
                - 'B'       business day frequency
                - 'C'       custom business day frequency (experimental)
                - 'D'       calendar day frequency
                - 'W'       weekly frequency
                - 'M'       month end frequency
                - 'SM'      semi-month end frequency (15th and end of month)
                - 'BM'      business month end frequency
                - 'CBM'     custom business month end frequency
                - 'MS'      month start frequency
                - 'SMS'     semi-month start frequency (1st and 15th)
                - 'BMS'     business month start frequency
                - 'CBMS'    custom business month start frequency
                - 'Q'       quarter end frequency
                - 'BQ'      business quarter endfrequency
                - 'QS'      quarter start frequency
                - 'BQS'     business quarter start frequency
                - 'A'       year end frequency
                - 'BA'      business year end frequency
                - 'AS'      year start frequency
                - 'BAS'     business year start frequency
                - 'BH'      business hour frequency
                - 'H'       hourly frequency
                - 'T', 'min' minutely frequency
                - 'S'       secondly frequency
                - 'L', 'ms' milliseconds
                - 'U', 'us' microseconds
                - 'N'       nanoseconds
                
            variable (str, optional): The variable for which the correlation anomalies should be detected.
                If not specified, the function will detect anomalies for all variables in the DataFrame.

            threshold (float, optional): The threshold for anomaly detection. If the absolute difference between
                the rolling correlation and the overall correlation exceeds this threshold, an anomaly is reported.
        """

        # Make a copy of the data and ensure that 'timestamp' is a datetime object.
        data_copy = self.data.copy()
        data_copy['timestamp'] = pd.to_datetime(data_copy['timestamp'])

        # Set 'timestamp' as the index of the copied DataFrame.
        data_copy.set_index('timestamp', inplace=True)

        # Check if specific variables should be analyzed or all
        if variables is None:
            variables = self.data.columns.tolist()  # convert to list
            if 'timestamp' in variables:
                variables.remove('timestamp')  # remove 'timestamp' from the list
        elif isinstance(variables, str):  # if only one variable is given, convert it to a list
            variables = [variables]

        # Calculate the overall correlation
        overall_corr = data_copy[variables].corr()

        # Resample the DataFrame into rolling windows and convert to a list of DataFrames
        rolling_windows = [group for _, group in data_copy.resample(rolling_window)]

        # For each window, calculate the correlation and compare it to the overall correlation
        for window in rolling_windows:
            window_corr = window[variables].corr()
            for variable in variables:
                variable_corr = window_corr.loc[variable].dropna()  # Correlation of 'variable' with all other variables
                overall_variable_corr = overall_corr.loc[variable].dropna()
                diff = variable_corr - overall_variable_corr
                anomalies = diff[np.abs(diff) > threshold]
                for other_variable, anomaly in anomalies.items():
                    print(f"Anomaly detected between {variable} and {other_variable} at {window.index[0]} - {window.index[-1]}: Correlation difference of {anomaly}")
    
    def __find_strong_correlations(self, correlation_matrix, threshold):
            """
            Internal method to find strong correlations in a correlation matrix.
            A strong correlation is defined as a correlation where the absolute value is larger than the threshold.

            Args:
                correlation_matrix (DataFrame): The correlation matrix.
                threshold (float): The threshold for strong correlations.

            Returns:
                A set of pairs of variables that have a strong correlation.
            """
            strong_correlations = dict()

            for column in correlation_matrix:
                for row in correlation_matrix.index:
                    if column != row: # to avoid self-correlation
                        correlation = correlation_matrix.loc[row, column]
                        if np.abs(correlation) > threshold: # consider absolute correlation values
                            pair = frozenset((row, column)) # use frozenset to avoid duplicate pairs
                            if pair not in strong_correlations:
                                strong_correlations[pair] = correlation

            return set(strong_correlations.keys())
    
    def compare_correlations(self, data_list, threshold, names=None, plot=False):
        """
        Compare correlations in internal data of this AnalyticsHandler object with the data provided in the input.

        Args:
            data_list (list): A list of DataFrame objects or AnalyticsHandler objects whose data should be compared
                            with this AnalyticsHandler's internal data.
            threshold (float): The threshold for strong correlations. Only correlations with an absolute value larger
                            than the threshold are considered strong.
            names (list, optional): A list of names for the data sets in data_list for printing. If not provided,
                                    the datasets are referred to as 'dataset 1', 'dataset 2', etc.
            plot (bool, optional): If True, a scatter plot for each pair of strongly correlated variables is shown.
                                    Default is False.
        """
        # Add the internal data of the AnalyticsHandler object to the list of data to be compared
        data_list = [data.data if isinstance(data, AnalyticsHandler) else data for data in data_list]
        data_list.insert(0, self.data)

        # Create correlation matrices for each dataset
        correlation_matrices = [data.corr() for data in data_list]

        # Find strong correlations in each dataset
        strong_correlations = [self.__find_strong_correlations(cm, threshold) for cm in correlation_matrices]

        # If there are no strong correlations found, end the function
        if not any(strong_correlations):
            print("No strong correlations found.")
            return

        # Find common correlations
        common_correlations = set.intersection(*strong_correlations)

        # If names are not provided, create default names
        if names is None:
            names = ['dataset ' + str(i+1) for i in range(len(data_list))]

        # Print common correlations
        print("Common correlations:")
        for pair in common_correlations:
            var1, var2 = list(pair)
            print(f"Correlation between {var1} and {var2}: {correlation_matrices[0].loc[var1, var2]}")
            if plot:
                for i, data in enumerate(data_list):
                    data.plot.scatter(var1, var2)
                    plt.title(f"Scatter plot of {var1} vs {var2} in {names[i]}")
                    plt.show()

        # Find unique correlations in each dataset and print them
        for i, unique_correlations in enumerate(strong_correlations):
            unique_correlations -= common_correlations
            print(f"\nUnique correlations in {names[i]}:")
            for pair in unique_correlations:
                var1, var2 = list(pair)
                print(f"Correlation between {var1} and {var2}: {correlation_matrices[i].loc[var1, var2]}")
                if plot:
                    data_list[i].plot.scatter(var1, var2)
                    plt.title(f"Scatter plot of {var1} vs {var2} in {names[i]}")
                    plt.show()

