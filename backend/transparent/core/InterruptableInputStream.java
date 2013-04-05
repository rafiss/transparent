package transparent.core;

import java.io.IOException;
import java.io.InputStream;

public class InterruptableInputStream extends InputStream
{
	private InputStream in;
	private Interruptable interruptable;
	private int sleepDuration;
	
	public InterruptableInputStream(InputStream in,
			Interruptable interruptable, int sleepDuration)
	{
		this.in = in;
		this.interruptable = interruptable;
		this.sleepDuration = sleepDuration;
	}
	
	@Override
	public int read() throws IOException
	{
		while (in.available() == 0) {
			try {
				Thread.sleep(sleepDuration);
			} catch (InterruptedException e) {
				throw new InterruptedStreamException(e);
			}
			
			if (in.available() == 0 && interruptable.interrupted())
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
