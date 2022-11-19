package org.apache.spark.sql.hbase

import org.apache.spark.annotation.{Experimental, Stable}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EvalSubqueriesForTimeTravel, ReplaceCharWithVarchar, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener
import org.apache.spark.sql.catalyst.parser.ParserInterface
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
import org.apache.spark.sql.hbase.execution.HBaseSqlParser
import org.apache.spark.sql.hbase.execution.datasources.HBaseOnlyCheck
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SparkUDFExpressionBuilder}
import org.apache.spark.sql.{SparkSession, Strategy}

/**
 * Created by wpy on 17-5-17.
 */
@Experimental
@Stable
class HBaseSessionStateBuilder(
                                hbaseSession: HBaseSession,
                                parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(hbaseSession.asInstanceOf[SparkSession], parentState) {

  //  override protected lazy val sqlParser: ParserInterface = extensions.buildParser(session, new HBaseSqlParser())

  override protected lazy val catalog: HBaseSessionCatalog = {
    val catalog = new HBaseSessionCatalog(
      () => externalCatalog,
      () => hbaseSession.sharedState.globalTempViewManager,
      functionRegistry,
      tableFunctionRegistry,
      SessionState.newHadoopConf(hbaseSession.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      new SparkUDFExpressionBuilder)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: ExternalCatalogWithListener =
    hbaseSession.sharedState.externalCatalog


  /**
   * A logical query plan `Analyzer` with rules specific to Hive.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalogManager) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = {
      new ResolveHBaseTable(hbaseSession) +:
        new FindDataSourceTable(hbaseSession) +:
        new ResolveSQLOnFile(hbaseSession) +:
        new FallBackFileSourceV2(hbaseSession) +:
        ResolveEncodersInScalaAgg +:
        new ResolveSessionCatalog(catalogManager) +:
        ResolveWriteToStream +:
        new EvalSubqueriesForTimeTravel +:
        customResolutionRules
    }

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      DetectAmbiguousSelfJoin +:
        PreprocessTableCreation(hbaseSession) +:
        PreprocessTableInsertion +:
        HBaseAnalysis +:
        DataSourceAnalysis(this) +:
        ApplyCharTypePadding +:
        ReplaceCharWithVarchar +:
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
    new SparkPlanner(hbaseSession, experimentalMethods) with HBaseStrategies {
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
            BasicOperators :: Nil)
      }
    }
  }

  override protected def newBuilder: NewBuilder = (session, _) => {
    val hs = new HBaseSession(session.sparkContext, session.sparkContext.hadoopConfiguration, Map.empty)
    new HBaseSessionStateBuilder(hs)
  }
}
