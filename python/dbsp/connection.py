import dbsp_api_client

from dbsp_api_client.models.new_program_request import NewProgramRequest
from dbsp_api_client.api.program import list_programs
from dbsp_api_client.api.program import new_program
from dbsp_api_client.api.program import program_status
from dbsp.program import DBSPProgram

class DBSPConnection:
    """DBSP server connection.

    Args:
        url (str): URL of the DBSP server.
    """

    def __init__(self, url):
        self.api_client = dbsp_api_client.Client(
                base_url = url,
                timeout = 20.0)

        list_programs.sync_detailed(client = self.api_client).unwrap("Failed to fetch program list from the DBSP server")

    def open_program(self, *, name: str) -> DBSPProgram:
        """Open an existing program with the given name.  Program
        names must be unique, so there will only be one.

        Args:
            name (str): Program name

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProgram

        """

        open_program_response = program_status.sync_detailed(client = self.api_client, name=name).unwrap(f'Failed to find a program named "{name}"')

        return DBSPProgram(
            api_client=self.api_client,
            program_id=open_program_response.program_id,
            program_version=open_program_response.version)

    def create_program(self, *, name: str, sql_code: str, description: str = '') -> DBSPProgram:
        """Create a new program.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the program
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProgram
        """

        return self.create_program_inner(name = name, sql_code = sql_code, description = description, replace = False)

    def create_or_replace_program(self, *, name: str, sql_code: str, description: str = '') -> DBSPProgram:
        """Create a new program overwriting existing program with the same name, if any.

        If a program with the same name already exists, all pipelines associated
        with that program and the program itself will be deleted.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the program
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProgram
        """

        return self.create_program_inner(name = name, sql_code = sql_code, description = description, replace = True)

    def create_program_inner(self, *, name: str, sql_code: str, description: str, replace: bool):
        request = NewProgramRequest(name=name, overwrite_existing = replace, code=sql_code, description='')

        new_program_response = new_program.sync_detailed(client = self.api_client, json_body=request).unwrap("Failed to create a program")

        return DBSPProgram(
            api_client=self.api_client,
            program_id=new_program_response.program_id,
            program_version=new_program_response.version)
