import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.batch_extract_week3 as batch
from subprocess import CompletedProcess


class RefreshContractPipelineTest(unittest.TestCase):
    def test_refresh_contract_pipeline_calls_generator_then_validation(self) -> None:
        log = io.StringIO()
        generator_result = CompletedProcess(args=['generator'], returncode=0, stdout='generated\n', stderr='')
        validation_result = CompletedProcess(args=['runner'], returncode=0, stdout='validated\n', stderr='')

        with patch.object(batch, 'rerun_generator', return_value=generator_result) as mock_generator, \
             patch.object(batch, 'rerun_validation', return_value=validation_result) as mock_validation:
            result = batch.refresh_contract_pipeline(Path('outputs/week3/extractions.jsonl'), log)

        self.assertTrue(result)
        mock_generator.assert_called_once_with(batch.DEFAULT_CONTRACT_ID, Path('outputs/week3/extractions.jsonl'))
        mock_validation.assert_called_once_with(
            batch.DEFAULT_CONTRACT_ID,
            Path('outputs/week3/extractions.jsonl'),
            batch.DEFAULT_VALIDATION_REPORT,
        )
        self.assertIn('REGENERATE week3-document-refinery-extractions', log.getvalue())
        self.assertIn(f'VALIDATE {batch.DEFAULT_VALIDATION_REPORT}', log.getvalue())
        self.assertIn(f'OK validation report -> {batch.DEFAULT_VALIDATION_REPORT}', log.getvalue())

    def test_refresh_contract_pipeline_blocks_on_validation_failure(self) -> None:
        log = io.StringIO()
        generator_result = CompletedProcess(args=['generator'], returncode=0, stdout='', stderr='')
        validation_result = CompletedProcess(args=['runner'], returncode=1, stdout='', stderr='fail\n')

        with patch.object(batch, 'rerun_generator', return_value=generator_result), \
             patch.object(batch, 'rerun_validation', return_value=validation_result):
            result = batch.refresh_contract_pipeline(Path('outputs/week3/extractions.jsonl'), log)

        self.assertFalse(result)
        self.assertIn('downstream blocked', log.getvalue())

    def test_main_runs_refresh_pipeline_after_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / 'data'
            extracted_dir = tmp / 'extracted'
            week3_python = tmp / 'python.exe'
            output_path = tmp / 'outputs.jsonl'
            log_file = tmp / 'batch.log'
            data_dir.mkdir()
            extracted_dir.mkdir()
            week3_python.write_text('', encoding='utf-8')
            (data_dir / 'sample.pdf').write_text('pdf', encoding='utf-8')

            args = batch.argparse.Namespace(
                week3_repo=tmp,
                data_dir=data_dir,
                extracted_dir=extracted_dir,
                week3_python=week3_python,
                include_prefix=[],
                skip_name=[],
                output=output_path,
                timeout_seconds=1,
                log_file=log_file,
            )

            with patch.object(batch, 'parse_args', return_value=args), \
                 patch.object(batch, 'run_extractor', return_value=CompletedProcess(args=['extract'], returncode=0, stdout='ok\n', stderr='')), \
                 patch.object(batch, 'rerun_migration', return_value=CompletedProcess(args=['migrate'], returncode=0, stdout='migrated\n', stderr='')), \
                 patch.object(batch, 'refresh_contract_pipeline', return_value=True) as mock_refresh:
                exit_code = batch.main()

            self.assertEqual(0, exit_code)
            mock_refresh.assert_called_once()


if __name__ == '__main__':
    unittest.main()
