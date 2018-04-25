package de.tuberlin.dima.bdapro.solutions.gameoflife;

import java.io.Serializable;

public  class Cell implements Comparable<Cell>,Serializable {

		private int x;
    	private int y;
		private long id;



		public Cell() {
		}


		public void setY(int y) {
			this.y = y;
		}

		public void setId(long id) {
			this.id = id;
		}

		public long getId() {
			return this.toID();
		}

		public Cell(int x, int y) {
			this.x = x;
			this.y = y;
		}


		public int getX() {
			return x;
		}

		public void setX(int x) {
			this.x = x;
		}

		public int getY() {
			return y;
		}

		@Override
		public String toString() {
			return String.valueOf(x)+" "+String.valueOf(y);
		}
		public Long toID() {
			return Long.valueOf(x) + Long.valueOf(y);
		}



		@Override
		public int compareTo( final Cell o) {
			if(this.x==o.x){
				return Integer.compare(this.x, o.x);
			}
			return Integer.compare(this.x, o.x) + Integer.compare(this.y, o.y);
		}

	}