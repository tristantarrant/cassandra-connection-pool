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

import java.util.ArrayList;
import java.util.Collection;

public class OffsetArrayList<E> extends ArrayList<E> {
	int offset;
	
	public OffsetArrayList(Collection<? extends E> c, int offset) {
		super(c);
		this.offset = offset;
	}
	
	@Override
	public E get(int index) {
		return super.get((index+offset) % super.size());
	}
	

}
