import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class Test {
	public static void main(String[] args)
	{
		String text = "My name is roshan";
		int num = 10000;
		
		int a = 127;
		
		Byte b = 127;
		int bi = b.intValue();
		System.out.println(bi);
		System.out.println(a & bi);
		
		
		
		RandomAccessFile randomFile;
		try {
			randomFile = new RandomAccessFile("test.db", "rw");
			randomFile.writeBytes(text);
			randomFile.writeInt(num);
			num = num * 2;
			randomFile.writeInt(num);
			randomFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
