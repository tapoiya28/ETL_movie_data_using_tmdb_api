from ingest_api.database.repository.repository import BaseRepository
from ingest_api.clients.base_client import BaseAPIClient
from ingest_api.extractor.base_extractor import BaseExtractor

class CompanyExtractor(BaseExtractor):
    
    def __init__(self, client: BaseAPIClient, repository: BaseRepository):
        super().__init__(client, repository)

    def extract(self):
        query = """select distinct company_id from raw.raw_mov_comp_relation"""
        result = self.repository.db.execute(query)
        all_company = set(row[0] for row in result)  # Extract first column from each row
        existing_companies = self.repository.get_existing_ids()

        comp_ids = all_company.difference(existing_companies)
        print('company need to extract:', len(comp_ids))
        
        comps = []
        for comp_id in comp_ids:
            comp = self.client.get_detail_company(comp_id)
            comps.append(comp)

        return self.transform(comps)
        

    def transform(self, raw_companies: list[dict]):
        comps = []
        for comp in raw_companies:
            # Extract parent_company_id from dict
            if comp and isinstance(comp.get('parent_company'), dict):
                    comp['parent_company_id'] = comp['parent_company'].get('id')
            else:
                comp['parent_company_id'] = None
            comp.pop('parent_company', None)  # Remove the dict field
            
            comps.append(comp)
        return comps

    def load(self, data):
        self.repository.save(data)
