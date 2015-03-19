package com.dangets.avroschemaevolution;

import com.dangets.avroschemaevolution.avro.User1;
import com.dangets.avroschemaevolution.avro.User2;
import java.io.File;
import java.io.IOException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class Main
{
	public static void testUser1ToUser2() throws IOException
	{
		User1 user1 = User1.newBuilder()
			.setName("user1")
			.setAge(42)
			.setFavoriteNumber(256)
			.setFavoriteColor("green")
			.build();

		DatumWriter<User1> dw = new SpecificDatumWriter<User1>(User1.class);
		DataFileWriter<User1> fw = new DataFileWriter<User1>(dw);
		fw.create(user1.getSchema(), new File("u1Tou2.avro"));
		fw.append(user1);
		fw.close();

		DatumReader<User2> dr = new SpecificDatumReader<User2>(User2.class);
		DataFileReader<User2> fr = new DataFileReader<User2>(new File("u1Tou2.avro"), dr);
		User2 user2 = null;
		while (fr.hasNext()) {
			try {
				user2 = fr.next(user2);
				System.out.println(user2);
			} catch (AvroTypeException | IOException ex) {
				System.err.println("unable to parse user2 obj: " + ex.getLocalizedMessage());
				break;
			}
		}
	}

	public static void main(String[] args) throws IOException
	{
		testUser1ToUser2();
	}
}