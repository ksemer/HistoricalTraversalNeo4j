package singleTypeIntervalEdge;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

import system.Queries;
import system.GraphDbTraversal;

/**
 * SingleIntervalTraversal class
 * 
 * @author ksemer
 */
public class SingleIntervalTraversal extends GraphDbTraversal {

	// adjacency relationship
	private RelationshipType adjRelation;

	/**
	 * Constructor
	 * 
	 * @param graphdb
	 */
	public SingleIntervalTraversal(GraphDatabaseService graphdb) {
		super(graphdb);

		// load graph types
		loadTypes();
	}

	/******************************** Util ********************************/

	@Override
	protected void loadTypes() {

		try (Transaction tx = graphdb.beginTx()) {

			for (RelationshipType rel : graphdb.getAllRelationshipTypes()) {

				if (rel.name().equals("" + RelTypes.EXISTS))
					continue;

				adjRelation = rel;
			}

			for (IndexDefinition x : graphdb.schema().getIndexes()) {
				System.out.println(x.getPropertyKeys() + "\t" + x.getLabel());
			}

			tx.success();
		}
	}

	@Override
	protected BitSet getLifespan(Relationship rel, BitSet I) {
		BitSet lifespan = new BitSet();
		String[] interval;
		int start, end, j_ = 0;

		int[] tstart = (int[]) rel.getProperty("tstart");
		int[] tend = (int[]) rel.getProperty("tend");

		for (int i = 0; i < Queries.intervals.length; i++) {
			interval = Queries.intervals[i].split("-");
			start = Integer.parseInt(interval[0]);
			end = Integer.parseInt(interval[1]);

			for (int j = j_; j < tstart.length; j++) {

				if (tend[j] < start) {
					if (j + 1 == tstart.length)
						break;

					continue;
				}

				if (tstart[j] > end)
					break;

				if (Math.max(tstart[j], start) <= Math.min(tend[j], end)) {

					// if query interval is subinterval of edge lifespan
					if (start >= tstart[j] && end <= tend[j])
						lifespan.set(start, end + 1);
					// if edge lifespan is subinterval of query interval
					else if (tstart[j] >= start && tend[j] <= end)
						lifespan.set(tstart[j], tend[j] + 1);
					else {
						for (int i_ = I.nextSetBit(tstart[j]); i_ >= 0; i_ = I.nextSetBit(i_ + 1)) {
							if (i_ >= tstart[j] && i_ <= tend[j])
								lifespan.set(i_);
						}
					}

					j_ = j;
				}
			}
		}

		lifespan.and(I);

		return lifespan;
	}

	@Override
	public List<RelationshipType> getAdjRelation() {
		List<RelationshipType> rels = new ArrayList<>(1);
		rels.add(adjRelation);

		return rels;
	}
}