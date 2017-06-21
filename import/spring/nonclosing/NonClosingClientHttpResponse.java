package no.ssb.vtl.tools.sandbox.connector.spring.nonclosing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.AbstractClientHttpResponse;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * A wrapper that prevent the InputStream from .
 */
public abstract class NonClosingClientHttpResponse extends AbstractClientHttpResponse {

    Logger log = LoggerFactory.getLogger(NonClosingClientHttpResponse.class);

    protected abstract ClientHttpResponse delegate();

    @Override
    public int getRawStatusCode() throws IOException {
        try {
            return delegate().getRawStatusCode();
        } catch (IOException ioe) {
            delegate().close();
            throw ioe;
        }
    }

    @Override
    public String getStatusText() throws IOException {
        try {
            return delegate().getStatusText();
        } catch (IOException ioe) {
            delegate().close();
            throw ioe;
        }
    }

    @Override
    public void close() {
        log.debug("original call to close avoided");
    }

    @Override
    public InputStream getBody() throws IOException {

        try {
            java.io.InputStream original = delegate().getBody();
            return new InputStream() {

                @Override
                protected java.io.InputStream delegate() {
                    return original;
                }

                @Override
                public void closeDelegate() throws IOException {
                    log.debug("original call to close");
                    original.close();
                }
            };
        } catch (IOException ioe) {
            delegate().close();
            throw ioe;
        }
    }

    @Override
    public HttpHeaders getHeaders() {
        return delegate().getHeaders();
    }

    abstract class InputStream extends java.io.InputStream {

        Thread owner = Thread.currentThread();

        protected abstract java.io.InputStream delegate();

        public abstract void closeDelegate() throws IOException;

        @Override
        public synchronized void close() throws IOException {
            log.debug("prevented {} to be closed", this);
        }

        @Override
        public int read() throws IOException {
            return delegate().read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return delegate().read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate().read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return delegate().skip(n);
        }

        @Override
        public int available() throws IOException {
            return delegate().available();
        }

        @Override
        public synchronized void mark(int readlimit) {
            delegate().mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            delegate().reset();
        }

        @Override
        public boolean markSupported() {
            return delegate().markSupported();
        }
    }
}
