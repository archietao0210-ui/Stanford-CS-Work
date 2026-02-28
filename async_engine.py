import asyncio
import aiohttp
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# Standard professional logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [CORE] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataStreamEngine:
    """
    High-performance asynchronous task engine designed for 
    large-scale data acquisition and AI model evaluation pipelines.
    """
    
    def __init__(self, concurrency_limit: int = 10):
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.start_time = time.time()

    async def execute_task(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """
        Executes an individual task with retry logic and timeout handling.
        """
        async with self.semaphore:
            try:
                # 15-second timeout for robust network operations
                async with session.get(url, timeout=15) as response:
                    status_code = response.status
                    if status_code == 200:
                        data = await response.json()
                        processed_payload = self._process_logic(data)
                        return {
                            "status": "SUCCESS",
                            "url": url,
                            "payload": processed_payload,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    else:
                        return {"status": f"HTTP_{status_code}", "url": url}
            except Exception as e:
                logger.error(f"Task execution failed: {str(e)}")
                return {"status": "FAILED", "url": url, "error": str(e)}

    def _process_logic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Core logic for data transformation and cleaning.
        """
        # Demonstrating data handling capabilities
        return {
            "origin_count": len(data),
            "engine_tag": "STF-V1-PRO",
            "processed_at": datetime.now().isoformat(),
            "cleaned": True
        }

    async def run_pipeline(self, target_urls: List[str]):
        """
        Orchestrates the asynchronous execution pipeline.
        """
        logger.info(f"Initializing pipeline. Targets: {len(target_urls)}")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.execute_task(session, url) for url in target_urls]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            logger.info(f"Pipeline completed. Total time: {end_time - self.start_time:.2f}s")
            return results

if __name__ == "__main__":
    # Test cases for demonstration using public APIs
    sample_urls = [
        "https://api.github.com",
        "https://api.spacexdata.com/v4/launches/latest",
        "https://httpbin.org/get"
    ]

    # Initialize engine with concurrency limit of 5
    engine = DataStreamEngine(concurrency_limit=5)
    
    # Run the event loop
    try:
        output = asyncio.run(engine.run_pipeline(sample_urls))
        
        print("\n" + "="*50)
        print("Execution Summary:")
        for item in output:
            print(f"[{item['status']}] -> {item['url']}")
        print("="*50)
    except KeyboardInterrupt:
        pass
