import mysql.connector

class MySQLManager:
    def __init__(self, host='localhost', user='root', password='root'):
        """Establishes a connection to the MySQL server.

        Args:
            host (str, optional): MySQL host. Defaults to "localhost".
            user (str, optional): MySQL username. Defaults to "root".
            password (str, optional): MySQL password. Defaults to "root".
        """
        self.host = host
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        """Connects to the MySQL server."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            print("Connected to MySQL Server!")
        except mysql.connector.Error as err:
            print(f"Error connecting to MySQL Server: {err}")

    def close_connection(self):
        """Closes the connection to the MySQL server."""
        if self.connection:
            self.connection.close()
            print("Connection to MySQL Server closed!")

    def create_user(self, user ="airq", password ="airq"):
        """Creates a new MySQL user with the provided username and password.

        Args:
            user (str, optional): DB-Username. Defaults to "airq".
            password (str, optional): DB-Password. Defaults to "airq".
        """
        try:
            cursor = self.connection.cursor()

            # Create a new user with the provided password
            cursor.execute(f"CREATE USER IF NOT EXISTS '{user}'@'localhost' IDENTIFIED BY '{password}'")
            print(f"User '{user}' created!")

            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error creating user: {err}")

    def create_database(self, database = "airq_data"):
        """Creates a new MySQL database.

        Args:
            database (str, optional): DB-Name. Defaults to "airq_data".
        """
        try:
            cursor = self.connection.cursor()

            # Create a new database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            print(f"Database '{database}' created!")

            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error creating database: {err}")

    def grant_privileges(self, user, database):
        """Grants all privileges to a user on a specific database.

        Args:
            user (str): User to whom the privileges will be granted.
            database (str): Database on which the privileges will be granted.
        """
        try:
            cursor = self.connection.cursor()

            # Grant privileges to the user on the database
            cursor.execute(f"GRANT ALL PRIVILEGES ON {database}.* TO '{user}'@'localhost'")
            print(f"Privileges granted to '{user}' on '{database}'!")

            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error granting privileges: {err}")

    def check_user_exists(self, user):
        """Checks if a user already exists in the MySQL server.

        Args:
            user (str): User to check.

        Returns:
            bool: True if the user exists, False otherwise.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = '{user}')")
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def check_database_exists(self, database):
        """Checks if a database already exists in the MySQL server.

        Args:
            database (str): Database to check.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        result = cursor.fetchone()
        cursor.close()
        return bool(result)

    def delete_user(self, user):
        """Deletes a MySQL user.

        Args:
            user (str): User to delete.
        """
        try:
            cursor = self.connection.cursor()

            # Delete the user
            cursor.execute(f"DROP USER IF EXISTS '{user}'@'localhost'")
            print(f"User '{user}' deleted!")

            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error deleting user: {err}")

    def delete_database(self, database):
        """Deletes a MySQL database.

        Args:
            database (str): Database to delete.
        """
        try:
            cursor = self.connection.cursor()

            # Delete the database
            cursor.execute(f"DROP DATABASE IF EXISTS {database}")
            print(f"Database '{database}' deleted!")

            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error deleting database: {err}")

    def get_existing_users(self):
        """Gets a list of existing MySQL users on the server.

        Returns:
            list: List of existing users.
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT User FROM mysql.user WHERE Host='localhost'")
        result = cursor.fetchall()
        cursor.close()
        return [user[0] for user in result]

    def get_existing_databases(self):
        """Gets a list of existing MySQL databases on the server.

        Returns:
            list: List of existing databases.
        """
        cursor = self.connection.cursor()
        cursor.execute("SHOW DATABASES")
        result = cursor.fetchall()
        cursor.close()
        return [database[0] for database in result]
