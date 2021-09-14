package org.apache.spark.sql.hbase

import org.apache.spark.annotation.{Experimental, Stable}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.adaptive.LogicalQueryStageStrategy
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Strategy, TableCapabilityCheck}
import org.apache.spark.sql.execution.streaming.ResolveWriteToStream
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}
import org.apache.spark.sql.{SparkSession, Strategy}

/**
 * Created by wpy on 17-5-17.
 */
@Experimental
@Stable
class HBaseSessionStateBuilder(
                                session: SparkSession,
                                parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  def this(hbaseSession: HBaseSession) = this(hbaseSession.asInstanceOf[SparkSession])

  override protected lazy val catalog: HBaseSessionCatalog = {
    val catalog = new HBaseSessionCatalog(
      () => externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      tableFunctionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: ExternalCatalogWithListener =
    session.sharedState.externalCatalog


  /**
   * A logical query plan `Analyzer` with rules specific to Hive.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalogManager) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = {
      new ResolveHBaseTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new FallBackFileSourceV2(session) +:
        ResolveEncodersInScalaAgg +:
        new ResolveSessionCatalog(catalogManager) +:
        ResolveWriteToStream +:
        customResolutionRules
    }

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      DetectAmbiguousSelfJoin +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion +:
        DataSourceAnalysis +:
        HBaseAnalysis +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        HiveOnlyCheck +:
        TableCapabilityCheck +:
        CommandCheck +:
        customCheckRules
  }

  override protected def planner: SparkPlanner = {
    new SparkPlanner(session, experimentalMethods) with HBaseStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++ (
          LogicalQueryStageStrategy ::
            PythonEvals ::
            HBaseTableScans ::
            HBaseDataSource ::
            new DataSourceV2Strategy(session) ::
            FileSourceStrategy ::
            DataSourceStrategy ::
            SpecialLimits ::
            Aggregation ::
            Window ::
            JoinSelection ::
            InMemoryScans ::
            SparkScripts ::
            WithCTEStrategy ::
            BasicOperators :: Nil)
      }
    }
  }

  override protected def newBuilder: NewBuilder = new HBaseSessionStateBuilder(_, _)
}
