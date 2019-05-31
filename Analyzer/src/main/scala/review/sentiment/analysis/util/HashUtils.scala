package review.sentiment.analysis.util

import java.security.{MessageDigest, DigestInputStream}
import java.io.{File, InputStream}

object HashUtils {

	// Compute a hash of a file
	// The output of this function should match the output of running "md5 -q <file>"
	def computeHash(is: InputStream): String = {
		val buffer = new Array[Byte](8192)
		val md5 = MessageDigest.getInstance("MD5")

		val dis = new DigestInputStream(is, md5)
		try {
			while (dis.read(buffer) != -1) {}
		} finally {
			dis.close()
		}

		md5.digest.map("%02x".format(_)).mkString
	}

}
