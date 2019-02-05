package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.Dataset;
import org.apache.parquet.io.InputFile;

import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static no.ssb.vtl.connector.parquet.io.LocalInputFile.nioPathToInputFile;

public class ParquetConnector implements Connector {

    public static final String STRUCTURE_META_NAME = "vtl.structure";
    public static final String DATAPOINT_TYPE_NAME = "no.ssb.vtl.datapoint";

    @Override
    public boolean canHandle(String identifier) {
        Path path = FileSystems.getDefault().getPath(identifier);
        return path.toFile().exists();
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        try {
            Path path = FileSystems.getDefault().getPath(identifier);
            InputFile inputFile = nioPathToInputFile(path);
            return new ParquetDataset(inputFile);
        } catch (FileNotFoundException e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        throw new ConnectorException("not implemented");
    }
}
