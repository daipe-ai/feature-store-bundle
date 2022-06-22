from typing import List, Dict, Union

from pyspark.dbutils import DBUtils


class MetadataHTMLDisplayer:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def display(self, metadata: List[Dict[str, Union[str, Dict[str, str]]]]):
        html = self.__get_html(metadata)
        self.__dbutils.notebook.displayHTML(html)

    def __get_html(self, metadata: List[Dict[str, Union[str, Dict[str, str]]]]) -> str:
        html = f"""
        <!doctype html>
              <html lang="en">
              <head>
                <meta charset="utf-8">
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.0/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-KyZXEAg3QhqLMpG8r+8fhAXLRk2vvoC2f3B09zVXn8CA5QIVfZOJ3BCsw2P0p/We" crossorigin="anonymous">
              </head>
              <body>
                <table class="table table-sm small">
                      <thead>
                        <tr>
                          <th scope="col">Name</th>
                          <th scope="col">Description</th>
                          <th scope="col">Extra</th>
                          <th scope="col">Template</th>
                          <th scope="col">Data Type</th>
                          <th scope="col">Variable Type</th>
                        </tr>
                      </thead>
                      <tbody>
                        {"".join(self.__get_table_row(metadata_dict) for metadata_dict in metadata)}
                      </tbody>
                    </table>
              </body>
            </html>
        """
        return html

    def __get_table_row(self, metadata_dict: Dict[str, Union[str, Dict[str, str]]]) -> str:
        # pyre-fixme[16]:
        return f"""
        <tr>
            <td>{metadata_dict["name"]}</td>
            <td>{metadata_dict["description"]}</td>
            <td>{", ".join(f"<b>{key}</b>: {val}" for key, val in metadata_dict["extra"].items())}</td>
            <td>{metadata_dict["feature_template"]}</td>
            <td>{metadata_dict["dtype"]}</td>
            <td>{metadata_dict["variable_type"]}</td>
        </tr>
        """
