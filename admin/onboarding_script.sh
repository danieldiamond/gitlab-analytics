## install homebrew
echo "Installing Homebrew.."
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
echo "Homebrew successfully installed"

## install git 
echo "Installing git.."
brew install git
echo "git successfully installed"

## install git completion
echo "Installing git completion.."
curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash > ~/git-completion.bash
echo 'source ~/git-completion.bash' >> ~/.bash_profile
echo "git completion successfully installed"

## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_1_7.zip > iTerm2.zip
unzip iTerm2.zip &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."
cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

## install atom
echo "Installing Atom.."
brew cask install atom
## this might ask you for your password
atom --version
echo 'export EDITOR="atom --wait"' >> ~/.bash_profile
echo "Atom successfully installed"

## install bash completion
echo "Installing bash competion.."
brew install bash-completion
echo "bash completion successfully installed"

## update terminal prompt
echo "Updating terminal prompt.."
echo 'GIT_PS1_SHOWDIRTYSTATE=1' >> ~/.bash_profile
echo 'GIT_PS1_SHOWUNTRACKEDFILES=1' >> ~/.bash_profile
curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh >> ~/git-prompt.sh
echo 'source ~/git-prompt.sh' >> ~/.bash_profile
echo "Terminal prompt successfully updated"

## create global gitignore
echo "Creating a global gitignore.."
git config --global core.excludesfile ~/.gitignore
touch ~/.gitignore
echo '.DS_Store' >> ~/.gitignore
echo '.idea' >> ~/.gitignore
echo "Global gitignore created"

## add functions to bash_rc
echo "Copying functions to make life easier.."
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml > ~/.bash_rc
source ~/.bash_rc
echo "Functions successfully copied"

## install the repo
echo "Installing the analytics repo.."
mkdir ~/repos/
cd ~/repos/
git clone git@gitlab.com:gitlab-data/analytics.git
echo "Analytics repo successfully installed"

## install goto
echo "Installing goto.."
brew install goto
echo -e "\$include /etc/inputrc\nset colored-completion-prefix on" >> ~/.inputrc
echo "goto successfully installed.. Adding alias for analytics.."
cd ~/repos/analytics
goto -r analytics .
echo "analytics goto alias successfully added"
## you can now type "goto analytics" and you're in the right place
## gl_open is now an alias to open this on gitlab.com

## install dbt
echo "Installing dbt.."
brew update
brew tap fishtown-analytics/dbt
brew install dbt
echo "dbt successfully installed.. Printing version.."
dbt --version
echo "Seting up dbt profile.."
mkdir ~/.dbt 
touch ~/.dbt/profiles.yml
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml >> ~/.dbt/profiles.yml
echo "dbt profile created.. You will need to edit this file later."
## you will need to edit this file

## install the dbt completion script
echo "Installing dbt completion script.."
curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > ~/.dbt-completion.bash
echo 'source ~/.dbt-completion.bash' >> ~/.bash_profile
echo "dbt completion script successfully installed"

echo 'source ~/.bashrc' >> ~/.bash_profile

source ~/.bash_profile

echo "Onboarding script run successfully."
