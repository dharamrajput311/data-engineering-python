from etl.base_extractor import BaseExtractor
from etl.base_transformer import BaseTransformer
from etl.base_loader import BaseLoader


class ETLPipeline:
    """
    Orchestrates Extract --> Transform --> Load
    """

    def __init__(
            self,
            extractor: BaseExtractor,
            transformer: BaseTransformer,
            valid_loader: BaseLoader,
            invalid_loader: BaseLoader
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.valid_loader = valid_loader
        self.invalid_loader = invalid_loader

    def run(self) -> None:
        raw_data = self.extractor.extract()

        valid_data, invalid_data = self.transformer.transform(raw_data) 

        context = {"source_file": self.extractor.file_path.name}

        if valid_data:
            self.valid_loader.load(records=valid_data)

        if invalid_data:
            self.invalid_loader.load(
                records=invalid_data,
                context=context
            )    
            

   
              

        
        

        


        


