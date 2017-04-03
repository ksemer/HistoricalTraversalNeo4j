package singleTypeIntervalEdge;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.graphdb.traversal.Uniqueness;

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

	@Override
	protected String pathTraversalAtLeast(Node src, Node trg, List<Integer> times, int k) {

		String path = "";
		BitSet interval = new BitSet();

		for (int t : times)
			interval.set(t);

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.NODE_PATH).evaluator(new Evaluator() {
					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");
						String[] interval = null;
						int start, end, j_ = 0;

						BitSet edge_lifespan = new BitSet();

						for (int i = 0; i < Queries.intervals.length; i++) {
							interval = Queries.intervals[i].split("-");
							start = Integer.parseInt(interval[0]);
							end = Integer.parseInt(interval[1]);

							for (int j = j_; j < tstart.length; j++) {

								if (tend[j] < start)
									continue;

								if (tstart[j] > end || tend[j] == start)
									break;

								if (start >= tstart[j] && end <= tend[j]) {
									edge_lifespan.set(start, end + 1);
									j_ = j;
									break;
								}
							}
						}

						if (edge_lifespan.cardinality() < k || path.endNode().equals(src))
							return Evaluation.EXCLUDE_AND_PRUNE;
						else if (path.endNode().equals(trg))
							return Evaluation.INCLUDE_AND_PRUNE;

						return Evaluation.INCLUDE_AND_CONTINUE;
					}

				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				BitSet I = (BitSet) interval.clone();
				String[] interval_;
				int j_, start, end;

				for (Relationship rel : p.relationships()) {
					BitSet life_ = new BitSet();

					int[] tstart = (int[]) rel.getProperty("tstart");
					int[] tend = (int[]) rel.getProperty("tend");
					j_ = 0;

					for (int i = 0; i < Queries.intervals.length; i++) {
						interval_ = Queries.intervals[i].split("-");
						start = Integer.parseInt(interval_[0]);
						end = Integer.parseInt(interval_[1]);

						for (int j = j_; j < tstart.length; j++) {

							if (tend[j] < start) {
								if (j + 1 == tstart.length) {
									break;
								}
								continue;
							}

							if ((tstart[j] > start && tstart[j] <= end) || tstart[j] >= end
									|| (tend[j] < end && tend[j] >= start)) {
								break;
							}

							if (start >= tstart[j] && end <= tend[j]) {
								life_.set(start, end + 1);
								j_ = j;
								break;
							}
						}
					}

					I.and(life_);

					if (I.cardinality() < k)
						break;
				}

				if (I.cardinality() >= k) {
					if (Queries.simplePath)
						path += Paths.simplePathToString(p);
					else
						path += Paths.pathToString(p, pathPrinter);
					return path;
				}
			}
		}

		return path;
	}

	protected String pathTraversalDisj(Node src, Node trg, int time_instance) {

		String path = "";

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");

						for (int i = 0; i < tstart.length; i++) {
							if (tstart[i] <= time_instance && time_instance <= tend[i])
								return Evaluation.INCLUDE_AND_CONTINUE;
							else if (tstart[i] > time_instance)
								return Evaluation.EXCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_PRUNE;
					}
				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				if (Queries.simplePath)
					path += Paths.simplePathToString(p);
				else
					path += Paths.pathToString(p, pathPrinter);
				return path;
			}
		}

		return path;
	}

	@Override
	protected String pathTraversalConj(Node src, Node trg, List<Integer> times) {

		String path = "";

		for (Path p : graphdb.traversalDescription().breadthFirst().relationships(adjRelation, Direction.BOTH)
				.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						if (path.length() == 0)
							return Evaluation.EXCLUDE_AND_CONTINUE;

						int[] tstart = (int[]) path.lastRelationship().getProperty("tstart");
						int[] tend = (int[]) path.lastRelationship().getProperty("tend");
						String[] interval = null;
						int start, end, j_ = 0;

						for (int i = 0; i < Queries.intervals.length; i++) {
							interval = Queries.intervals[i].split("-");
							start = Integer.parseInt(interval[0]);
							end = Integer.parseInt(interval[1]);

							for (int j = j_; j < tstart.length; j++) {

								if (tend[j] < start) {
									if (j + 1 == tstart.length) {
										return Evaluation.EXCLUDE_AND_PRUNE;
									}
									continue;
								}

								if ((tstart[j] > start && tstart[j] <= end) || tstart[j] >= end
										|| (tend[j] < end && tend[j] >= start)) {
									return Evaluation.EXCLUDE_AND_PRUNE;
								}

								if (start >= tstart[j] && end <= tend[j]) {
									j_ = j;
									break;
								}
							}
						}

						return Evaluation.INCLUDE_AND_CONTINUE;
					}
				}).traverse(src)) {

			if (p.endNode().equals(trg)) {
				if (Queries.simplePath)
					path += Paths.simplePathToString(p);
				else
					path += Paths.pathToString(p, pathPrinter);
				return path;
			}
		}

		return path;
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
						 for (int i_ = I.nextSetBit(tstart[j]); i_ >= 0; i_ = I.nextSetBit(i_+1)) {
						     if (i_ >= tstart[j] && i <= tend[j])
						    	 lifespan.set(i_);
						 }
					}

					j_ = j;
				}
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

	@Override
	protected String pathTraversalDisj(Node src, Node trg, List<Integer> times) {
		// TODO Auto-generated method stub
		return null;
	}
}