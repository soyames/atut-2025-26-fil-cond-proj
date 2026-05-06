from pathlib import Path
import sys
from unittest.mock import MagicMock, Mock, patch
import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# Pre-populate sys.modules with mock pyspark before any imports
class _MockDF(MagicMock):
    def withColumnRenamed(self, *args, **kwargs):
        return self
    
    def withColumn(self, *args, **kwargs):
        return self
    
    def select(self, *args, **kwargs):
        return self
    
    def dropDuplicates(self, *args, **kwargs):
        return self
    
    def join(self, *args, **kwargs):
        return self


def _create_mock_pyspark():
    """Create mock pyspark modules"""
    # Create mock F (functions) module
    mock_f = MagicMock()
    mock_f.udf = MagicMock(return_value=MagicMock())
    mock_f.lit = MagicMock(side_effect=lambda x: x)
    mock_f.col = MagicMock(return_value=MagicMock())
    mock_f.coalesce = MagicMock(return_value=MagicMock())
    
    # Create mock SparkSession
    mock_spark_session = MagicMock()
    mock_spark_session.read.option.return_value.csv.return_value = _MockDF()
    mock_spark_session.read.option.return_value.json.return_value = _MockDF()
    mock_spark_session.read.json.return_value = _MockDF()
    mock_spark_session.sparkContext.setLogLevel = MagicMock()
    mock_spark_session.stop = MagicMock()
    
    mock_builder = MagicMock()
    mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark_session
    
    # Create mock modules
    mock_pyspark = MagicMock()
    mock_sql = MagicMock()
    mock_sql.SparkSession = MagicMock()
    mock_sql.SparkSession.builder = mock_builder
    mock_sql.DataFrame = _MockDF
    mock_sql.functions = mock_f
    
    mock_py4j = MagicMock()
    mock_protocol = MagicMock()
    mock_protocol.Py4JJavaError = Exception
    
    return {
        'pyspark': mock_pyspark,
        'pyspark.sql': mock_sql,
        'pyspark.sql.functions': mock_f,
        'py4j': mock_py4j,
        'py4j.protocol': mock_protocol,
    }


# Inject mocks before importing test modules
_mocks = _create_mock_pyspark()
sys.modules.update(_mocks)


@pytest.fixture(autouse=True)
def reset_pyspark_mocks():
    """Ensure pyspark mocks are in place for each test"""
    sys.modules.update(_mocks)
    yield
    # Keep the mocks in place for the next test
