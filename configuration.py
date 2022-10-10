import json
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

class get_configuration():
    def get_config(newrelic_user_key,package_uuid,account_id,document_id):
        nr_headers = {'Content-Type': 'application/json', 'Api-Key': str(newrelic_user_key), 'NewRelic-Package-Id': str(package_uuid)}
        transport = AIOHTTPTransport(url="https://api.newrelic.com/graphql", headers=nr_headers)
        client = Client(transport=transport, fetch_schema_from_transport=True)
        query = gql(
            """
            query ($accountId: Int!, $documentId: String!){
            actor {
                account(id: $accountId ) {
                    nerdStorage {
                    document(collection: "SyntheticConfigurator", documentId: $documentId)
                    }
                }
            }
        }
        """
        )
        vars = {"accountId": int(account_id), "documentId": document_id }
        result = client.execute(query, variable_values=vars)
        config = json.loads(result['actor']['account']['nerdStorage']['document']['config'])
        return config['configuration']

if __name__ == "__main__":
    get_configuration.get_config()
