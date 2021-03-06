/**
 * Copyright 2010 Tristan Tarrant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dataforte.cassandra.pool;

/**
 * This enum was taken from Hector
 * 
 * @author Tristan Tarrant
 * 
 */
public enum HostFailoverPolicy {

	/**
	 * On communication failure, just return the error to the client and don't
	 * retry
	 */
	FAIL_FAST(0),
	/** On communication error try one more server before giving up */
	ON_FAIL_TRY_ONE_NEXT_AVAILABLE(1),
	/** On communication error try all known servers before giving up */
	ON_FAIL_TRY_ALL_AVAILABLE(Integer.MAX_VALUE - 1);

	public final int numRetries;

	HostFailoverPolicy(int numRetries) {
		this.numRetries = numRetries;
	}
}