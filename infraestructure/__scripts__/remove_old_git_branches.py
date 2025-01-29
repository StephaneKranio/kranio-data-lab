import subprocess


def run_command(command):
    command_list = command.split(' ')
    output = subprocess.run(command_list, capture_output=True, text=True)

    return output


class GitBranchesCleaner:
    def __init__(self):
        self.local_branches = None
        self.remote_branches = None
        self.branches_to_delete = None

    def get_remote_branches(self):
        cmd = 'git branch --remote'
        result = run_command(cmd).stdout
        result_list = result.split('\n')

        self.remote_branches = [
            value.strip().replace('origin/', '') for value in result_list
        ]
        self.remote_branches.remove('')

    def get_local_branches(self):
        cmd = 'git branch'
        result = run_command(cmd).stdout
        result_list = result.split('\n')

        self.local_branches = [value.strip('*').strip() for value in result_list]
        self.local_branches.remove('')

    def set_branches_to_delete(self):
        self.branches_to_delete = list(
            set(self.local_branches) - set(self.remote_branches)
        )
        self.branches_to_delete.sort()

    def delete_branches(self):
        delete_cmd = 'git branch -d '

        for branch_name in self.branches_to_delete:
            print(f'[INFO]: deleting {branch_name}...')
            command = delete_cmd + branch_name
            response = run_command(command)

            error_message = response.stderr
            if error_message:
                print('\n[ERROR]: ' + error_message + '\n')

    def clean(self):
        self.get_remote_branches()
        self.get_local_branches()
        self.set_branches_to_delete()
        self.delete_branches()


if __name__ == '__main__':
    GitBranchesCleaner().clean()
