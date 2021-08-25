from typing import List, Dict, Union


class MetadataHTMLDisplayer:
    def display(self, metadata: List[Dict[str, str]]):
        display_html = self.__get_display_html()
        html = self.__get_html(metadata)
        display_html(html)

    def __get_html(self, metadata: List[Dict[str, str]]) -> str:
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
                          <th scope="col">Type</th>
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
        return f"""
        <tr>
            <td>{metadata_dict["name"]}</td>
            <td>{metadata_dict["description"]}</td>
            <td>{", ".join(f"<b>{key}</b>: {val}" for key, val in metadata_dict["extra"].items())}</td>
            <td>{metadata_dict["template"]}</td>
            <td>{metadata_dict["dtype"]}</td>
        </tr>
        """

    def __get_display_html(self):
        import IPython

        ipython = IPython.get_ipython()

        if not hasattr(ipython, "user_ns") or "displayHTML" not in ipython.user_ns:
            raise Exception("displayHTML cannot be resolved")

        return ipython.user_ns["displayHTML"]
