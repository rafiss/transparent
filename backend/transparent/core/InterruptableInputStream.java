package transparent.core;

import java.io.IOException;
import java.io.InputStream;

public class InterruptableInputStream extends InputStream
{
	private static final int SLEEP_DURATION = 200;
	
	private InputStream in;
	private Interruptable interruptable;
	
	public InterruptableInputStream(InputStream in, Interruptable interruptable) {
		this.in = in;
		this.interruptable = interruptable;
	}
	
	@Override
	public int read() throws IOException
	{
		if (interruptable.interrupted())
			throw new InterruptedStreamException("Stream was interrupted"
					+ " by the underlying Interruptable.");

		while (in.available() == 0) {
			try {
				Thread.sleep(SLEEP_DURATION);
			} catch (InterruptedException e) {
				throw new InterruptedStreamException(e);
			}
			
			if (interruptable.interrupted())
				throw new InterruptedStreamException("Stream was interrupted"
						+ " by the underlying Interruptable.");
		}
		
		return in.read();
	}
	
	@Override
	public int available() throws IOException {
		return in.available();
	}
	
	@Override
	public void close() throws IOException {
		in.close();
	}
	
	@Override
	public synchronized void mark(int readlimit) {
		in.mark(readlimit);
	}
	
	@Override
	public synchronized void reset() throws IOException {
        in.reset();
    }
	
	@Override
	public boolean markSupported() {
        return in.markSupported();
    }
}
