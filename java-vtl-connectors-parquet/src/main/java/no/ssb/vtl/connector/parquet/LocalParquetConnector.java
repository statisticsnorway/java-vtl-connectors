package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.connector.parquet.io.LocalOutputFile;
import no.ssb.vtl.connectors.ConnectorException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static no.ssb.vtl.connector.parquet.io.LocalInputFile.nioPathToInputFile;

public class LocalParquetConnector extends ParquetConnector {
    @Override
    InputFile getFile(String identifier) throws ConnectorException {
        try {
            Path path = FileSystems.getDefault().getPath(identifier);
            InputFile inputFile = nioPathToInputFile(path);
            return inputFile;
        } catch (FileNotFoundException e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    OutputFile putFile(String identifier) throws ConnectorException {
        Path path = FileSystems.getDefault().getPath(identifier);
        return LocalOutputFile.nioPathToOutputFile(path);
    }
}
