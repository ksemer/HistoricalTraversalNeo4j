package singleTypePointsEdge;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

import system.GraphDbTraversal;

/**
 * SinglePointsTraversal class
 * 
 * @author ksemer
 */
public class SinglePointsTraversal extends GraphDbTraversal {

	// adjacency relationship
	private RelationshipType adjRelation;

	/**
	 * Constructor
	 * 
	 * @param graphdb
	 */
	public SinglePointsTraversal(GraphDatabaseService graphdb) {
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
		int[] timeInstances = (int[]) rel.getProperty("time_instances");
		int time_instance = -1, m = timeInstances.length, i = 0;

		Iterator<Integer> it = I.stream().iterator();

		if (it.hasNext())
			time_instance = it.next();

		while (i < m) {

			if (timeInstances[i] < time_instance)
				i++;
			else if (time_instance < timeInstances[i]) {
				if (it.hasNext())
					time_instance = it.next();
				else
					break;
			} else {
				lifespan.set(time_instance);

				if (it.hasNext())
					time_instance = it.next();
				else
					break;

				i++;
			}
		}

		return lifespan;
	}

	@Override
	public List<RelationshipType> getAdjRelation() {
		List<RelationshipType> rels = new ArrayList<>(1);
		rels.add(adjRelation);

		return rels;
	}
}